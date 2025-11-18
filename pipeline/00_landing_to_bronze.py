"""Landing (Auto Loader) to bronze ingestion for Coles and Woolworths."""

import dlt
from typing import List, Optional, Tuple
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T
from pyspark.sql.functions import current_timestamp, col, sha2, concat_ws
import re


catalog = 'workspace'
bronze_schema = '01_bronze'
silver_schema = '02_silver'
gold_schema = '03_gold'


concat_cols = concat_ws("||", *[col(column) for column in 
['Category', 'Name', 'DisplayName', 'Brand', 'Stockcode', 'Barcode', 'Price', 'WasPrice', 'SavingsAmount', 'Unit', 'CupPrice', 'InStock', 'IsOnSpecial', 'IsHalfPrice', 'Image', 'Description', 'URL', 'scrape_timestamp', 'batch_ts', 'scrape_run_id']
])

UNIT_PRICE_PATTERN = re.compile(
    r"""
    ^\s*\$?(?P<unit_price>\d+(?:\.\d+)?)   # amount (e.g. 1.35, 50.15)
    \s*/\s*
    (?P<unit_qty>\d+(?:\.\d+)?)?           # optional quantity before the unit (100, 1)
    (?P<unit_uom>[A-Za-z]+)                # unit letters (ML, G, EA, KG, L)
    \s*$
    """,
    re.VERBOSE,
)


def get_excluded_paths(conf_key: str = "pipeline.excluded_paths") -> List[str]:
    """
    Return a list of paths to exclude, driven by a comma-separated Spark conf.
    Example conf: pipeline.excluded_paths="s3://.../file1.csv,s3://.../file2.csv"
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        return []
    raw = spark.conf.get(conf_key, "")
    return [p.strip() for p in raw.split(",") if p.strip()]



@dlt.table(
    name=f"{catalog}.{bronze_schema}.coles_product_raw",
    comment="ingestion of raw data", 
    table_properties={"quality": "bronze"}
)
def ingest_coles_data():
    concat_cols = concat_ws("||", *[col(column) for column in [
        'cat', 'id', 'name', 'brand', 'description', 'size', 'availability', 'availabilityType', 'categoryGroup', 'category', 'subCategory', 'className', 'price_now', 'price_was', 'comparable', 'offerDescription', 'promotionType', 'specialType', 'unit_price', 'unit_measure', 'offer', 'scrape_timestamp', 'batch_ts', 'scrape_run_id'
    ]])

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("cloudFiles.schemaLocation","s3://grocery-raw-data/coles/schema/")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns") # needs to be together with writeStream 'mergeSchema'
        .option("cloudFiles.schemaHints", 
            "cat STRING, id STRING, name STRING, brand STRING, description STRING, size STRING, availability BOOLEAN, availabilityType STRING, categoryGroup STRING, category STRING, subCategory STRING, className STRING, price_now DOUBLE, price_was DOUBLE, comparable STRING, offerDescription STRING, promotionType STRING, specialType STRING, unit_price DOUBLE, unit_measure STRING, offer STRING, scrape_timestamp TIMESTAMP, batch_ts TIMESTAMP, scrape_run_id INT, isVerified STRING, storename STRING"
        )
        .option("header", True)
        .load("s3://grocery-raw-data/coles/2025/")
        .withColumn('source_file', col("_metadata.file_path"))
        .withColumn('ingestion_ts', current_timestamp())
        .withColumn('raw_hash', sha2(concat_cols, 256))
    )

    excluded = get_excluded_paths()
    if excluded:
        df = df.filter(~col("source_file").isin(excluded))

    return df



def parse_woolies_unit_price(text: str) -> Optional[Tuple[float, float, str]]:
    """Parse Woolworths unit price strings like '$1.50 / 100G' into numeric parts."""
    if not text:
        return None
    match = UNIT_PRICE_PATTERN.search(text.strip())
    if not match:
        return None
    price = float(match.group("unit_price"))
    qty = float(match.group("unit_qty") or 1)
    unit = match.group("unit_uom").lower()
    return price, qty, unit


parse_udf = F.udf(
    lambda text: parse_woolies_unit_price(text or "") or (0.0, 0.0, ""),
    T.StructType([
        T.StructField("unit_price", T.DoubleType()),
        T.StructField("unit_size", T.DoubleType()),
        T.StructField("unit_measure", T.StringType()),
    ]),
)

@dlt.table(
    name=f"{catalog}.{bronze_schema}.woolworths_product_raw",
    comment="ingestion of raw data", 
    table_properties={"quality": "bronze"}
)
def ingest_woolworths_data():
    concat_cols = concat_ws("||", *[col(column) for column in 
    ['Category', 'Name', 'DisplayName', 'Brand', 'Stockcode', 'Barcode', 'Price', 'WasPrice', 'SavingsAmount', 'Unit', 'CupPrice', 'InStock', 'IsOnSpecial', 'IsHalfPrice', 'Image', 'Description', 'URL', 'scrape_timestamp', 'batch_ts', 'scrape_run_id']
    ])

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("cloudFiles.schemaLocation","s3://grocery-raw-data/woolworths/schema/")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option(
            "cloudFiles.schemaHints",
            "Category STRING, Name STRING, DisplayName STRING, Brand STRING, "
            "Stockcode STRING, Barcode STRING, Price DOUBLE, WasPrice DOUBLE, "
            "SavingsAmount DOUBLE, Unit STRING, CupPrice STRING, InStock BOOLEAN, "
            "IsOnSpecial BOOLEAN, IsHalfPrice BOOLEAN, Image STRING, Description STRING, "
            "URL STRING, scrape_timestamp TIMESTAMP, batch_ts TIMESTAMP, "
            "scrape_run_id INT"
        )
        .option("header", True)
        .load("s3://grocery-raw-data/woolworths/2025/")
        .withColumn('source_file', col("_metadata.file_path"))
        .withColumn('ingestion_ts', current_timestamp())
        .withColumn('raw_hash', sha2(concat_cols, 256))
    )

    excluded = get_excluded_paths()
    if excluded:
        df = df.filter(~col("source_file").isin(excluded))

    parsed_cols = (
        df
        .withColumn("parsed_unit", parse_udf(F.col("CupPrice")))
    )
    parsed_cols = (
        parsed_cols
    .withColumn("unit_price", F.col("parsed_unit.unit_price"))
        .withColumn("unit_size", F.col("parsed_unit.unit_size"))
        .withColumn("unit_uom", F.col("parsed_unit.unit_measure"))
        .drop("parsed_unit")
    )

    return parsed_cols
