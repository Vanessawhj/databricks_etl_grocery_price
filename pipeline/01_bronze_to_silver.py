"""Bronze → Silver staging transforms with normalization and SCD2 staging."""

import re
from pyspark.sql.functions import lit, create_map
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, coalesce, udf, col
from pyspark.sql.types import StringType, FloatType, StructType, StructField
from pyspark import pipelines as dp


catalog = 'workspace'
bronze_schema = '01_bronze'
silver_schema = '02_silver'
gold_schema = '03_gold'


SIZE_RE = re.compile(r'(\d+(?:\.\d+)?)\s*(kg|g|l|ml)', re.I)
PACK_RE = re.compile(r'(\d+)\s*(pack|pk|pkt|each)', re.I)


def normalize_name(name: str) -> str:
    name = re.sub(r"<br\s*/?>", " ", name, flags=re.I)  # strip HTML <br>
    name = re.sub(r"\.{2,}", " ", name)        # turn "..." into a space
    name = name.lower()
    name = re.sub(r'[®™]', '', name)                  # remove symbols
    name = re.sub(r'\s+', ' ', name)                  # collapse whitespace
    name = re.sub(r':+\.?:*', ' ', name)   # replace any mix of ':' or '.' with space
    name = re.sub(r'\s+', ' ', name).strip()  # collapse multiple spaces

    return name

def parse_pack_size_and_pack(name: str):

    m = SIZE_RE.search(name)
    n = PACK_RE.search(name)

    if m:
        qty = float(m.group(1))
        unit = m.group(2).lower()
        return qty, unit
    elif n:
        qty = float(n.group(1))
        unit = n.group(2).lower()
        return qty, unit
    
    else:
        return None, None
    

COLES_CATEGORY_MAP = {
        "fruit & vegetables": "fruits_veg",
        "chips, chocolates & snacks": "snacks_confectionary",
        "frozen": "freezer",
        "drinks": "drinks",
        "liquorland": "alcohol",
        "dairy, eggs & fridge": "dairy_eggs_fridge",
        "home & garden": "home_garden",
        "meat & seafood": "poultry_meat_seafood",
        "pantry": "pantry",
        "dietary & world foods": "international_food",
        'cleaning & laundry': 'cleaning_laundry',
        'health & beauty': 'health_beauty',
        'pet': 'pet',
        'baby': 'baby',
        'bakery': 'bakery',
        'deli': 'deli'
    }


WOOLWORTHS_CATEGORY_MAP = {
        'lunch_box': 'lunch_box',
        'drinks': 'drinks',
        'fruits_veg': 'fruits_veg', 
        'dinner': 'dinner',
        'deli': 'deli',
        'pantry': 'pantry',
        'bakery': 'bakery',
        'freezer': 'freezer',
        'snacks_confectionary': 'snacks_confectionary', 
        'international_food': 'international_food',
        'half_price': 'half_price',
        'poultry_meat_seafood': 'poultry_meat_seafood',
        'dairy_eggs_fridge': 'dairy_eggs_fridge',
        'beer_wine_spirits': 'alcohol'
    }


normalize_name_udf = udf(normalize_name, StringType())

parse_pack_size_and_pack_udf = udf(parse_pack_size_and_pack,
                                StructType([
                                    StructField('unit_size', FloatType()),
                                    StructField('unit_uom', StringType())   
                                ]))


# need to flatten the list comprehension for create_map.
coles_mapping = create_map(
    *[item for k, v in COLES_CATEGORY_MAP.items() for item in (lit(k), lit(v))])
woolworths_mapping = create_map(
    *[item for k, v in WOOLWORTHS_CATEGORY_MAP.items() for item in (lit(k), lit(v))])


@dp.view
@dp.expect_or_drop("non_null_scrape_timestamp", "scrape_timestamp IS NOT NULL")
@dp.expect_or_drop("valid_store_id_coles", "store_id = 1000")
@dp.expect_or_drop("category_mapped_coles", "category_name IS NOT NULL")
@dp.expect_or_drop("sku_present_coles", "store_sku IS NOT NULL")
# @dp.expect_or_drop("valid_price", "regular_price > 0")
def stage_coles_data():

    df = (
        spark.readStream
        .table(f"{catalog}.{bronze_schema}.coles_product_raw")
        .withColumn('store_id', lit(1000)) # literal
        .withColumn('scrape_timestamp', F.to_timestamp(col('scrape_timestamp'), 'yyyy-MM-dd HH:mm:ss'))
        # .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn('name', coalesce('name','description'))
        .withColumnRenamed('id','store_sku')
        .withColumnRenamed("name", "store_product_name")
        .withColumnRenamed("brand", "brand_name")
        .withColumn('category_name', coles_mapping[col('cat')])
        # .filter(~col("source_file").contains("s3://grocery-raw-data/coles/2025/11/16/coles_product_20251116.csv"))

        .drop('cat')
    )


    df = (df
        .withColumn('canonical_name', normalize_name_udf(coalesce('description', lit(''))))
        .withColumn('pack_struct', parse_pack_size_and_pack_udf('canonical_name'))
        )

    df = (df
        .withColumn("unit_size", F.col("pack_struct.unit_size").cast("double"))
        .withColumn("unit_uom", F.col("pack_struct.unit_uom"))
        .drop('pack_struct')
    )


    staged_cols = [
                "store_id",
                "store_sku",
                "store_product_name",
                "description",
                "canonical_name",
                "brand_name",
                "category_name",
                # "unit_price",
                "unit_size",
                "unit_uom",
                "raw_hash",
                "ingestion_ts",
                "scrape_timestamp"
            ]

    df = df.select(*staged_cols)



    return df




dp.create_streaming_table(
    name=f"{catalog}.{silver_schema}.coles_product_stage",
    schema="""
        store_id INT NOT NULL,
        store_sku STRING,
        store_product_name STRING,
        description STRING,
        canonical_name STRING,
        brand_name STRING,
        category_name STRING,
        unit_size DOUBLE,
        unit_uom STRING,
        raw_hash STRING,
        ingestion_ts TIMESTAMP,
        scrape_timestamp TIMESTAMP,
        __START_AT TIMESTAMP,
        __END_AT TIMESTAMP
    """,
    table_properties={"delta.enableChangeDataFeed": "true"}
)


@dp.view
@dp.expect_or_drop("valid_store_id_woolies", "store_id = 1001")
@dp.expect_or_drop("category_mapped_woolies", "category_name IS NOT NULL")
@dp.expect_or_drop("sku_present_woolies", "store_sku IS NOT NULL")
def stage_woolies_data():

    df_woolworths = (
        spark.readStream
        .table(f"{catalog}.{bronze_schema}.woolworths_product_raw")
        .withColumn('store_id', lit(1001)) # literal
        .withColumnRenamed('name','store_product_name')
        .withColumnRenamed("brand", "brand_name")
        .withColumnRenamed("Stockcode", "store_sku")
        .withColumn('category_name', woolworths_mapping[col('category')])
        .drop('category')
    )


    df_woolworths = (
        df_woolworths
        .withColumn('canonical_name', normalize_name_udf(coalesce('description', lit(''))))
        .withColumn('pack_struct', parse_pack_size_and_pack_udf('canonical_name'))
    )


    df_woolworths = (df_woolworths
        .withColumn("unit_size", F.col("pack_struct.unit_size").cast("double"))
        .withColumn("unit_uom", F.col("pack_struct.unit_uom"))
        .drop('pack_struct')
    )
    staged_cols = [
                "store_sku",
                "store_id",
                "store_product_name",
                "brand_name",
                "category_name",
                "canonical_name",
                "unit_size",
                "unit_uom",
                "raw_hash",
                "ingestion_ts",
                # "norm_hash",
            ]


    df_woolworths = df_woolworths.select(*staged_cols)
    return df_woolworths




dp.create_streaming_table(
    name=f"{catalog}.{silver_schema}.woolworths_product_stage",
    schema="""
        store_id INT NOT NULL,
        store_sku STRING,
        store_product_name STRING,
        description STRING,
        canonical_name STRING,
        brand_name STRING,
        category_name STRING,
        unit_size DOUBLE,
        unit_uom STRING,
        raw_hash STRING,
        ingestion_ts TIMESTAMP,
        __START_AT TIMESTAMP,
        __END_AT TIMESTAMP
    """,
    table_properties={"delta.enableChangeDataFeed": "true"}
)


dp.create_auto_cdc_flow(
    target=f"{catalog}.{silver_schema}.coles_product_stage",
    source="stage_coles_data",
    keys=["store_id","store_sku"],
    sequence_by=col("ingestion_ts"),
    stored_as_scd_type=2,         
)

dp.create_auto_cdc_flow(
    target=f"{catalog}.{silver_schema}.woolworths_product_stage",
    source="stage_woolies_data",
    keys=["store_id","store_sku"],
    sequence_by=col("ingestion_ts"),
    stored_as_scd_type=2,         
)
