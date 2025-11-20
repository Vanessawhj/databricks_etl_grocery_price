"""Fuzzy matching Woolworths products to canonical dimension."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F, types as T
from rapidfuzz import process, fuzz
import pandas as pd

catalog = "workspace"
silver_schema = "02_silver"
MATCH_SCORE_THRESHOLD = 80
canonical_table = f"{catalog}.{silver_schema}.canonical"


schema = T.StructType([
    T.StructField("brand_name", T.StringType()),
    T.StructField("category_name", T.StringType()),
    T.StructField("brand_id", T.LongType()),
    T.StructField("category_id", T.LongType()),
    T.StructField("canonical_name", T.StringType()),
    T.StructField("store_sku", T.StringType()),
    T.StructField("store_id", T.IntegerType()),
    T.StructField("canonical_product_id", T.LongType()),
    T.StructField("unit_size", T.DoubleType()),
    T.StructField("unit_uom", T.StringType()),
    T.StructField("match_score", T.DoubleType()),
    T.StructField("match_status", T.StringType()),
    T.StructField("ingestion_ts",T.TimestampType())
])

expected_cols = [
    "brand_name", "category_name", "brand_id", "category_id", "canonical_name",
    "store_sku", "store_id", "canonical_product_id", "unit_size", "unit_uom",
    "match_score", "match_status", "ingestion_ts"
    # , "canonical_name_candidate","canonical_product_id_candidate"
]


store_col = ['store_id', 'brand_id', 'store_sku', 'category_id', 'unit_size', 'unit_uom','canonical_product_id','canonical_name','match_status','match_score','category_name','brand_name', 'ingestion_ts'
# ,"canonical_name_candidate","canonical_product_id_candidate"
]

@dp.view()
def woolworths_match_stage():
    """Block (brand/category) Woolworths SKUs and fuzzy match to canonical names."""
    brand_dim = spark.read.table(f"{catalog}.{silver_schema}.dim_brand").drop("ingestion_ts")
    category_dim = spark.read.table(f"{catalog}.{silver_schema}.dim_category").drop("ingestion_ts")
    canonical_df = spark.read.table("coles_canonical")

    woolies = (
        spark.read.table(f"{catalog}.{silver_schema}.woolworths_product_stage")
        .join(brand_dim, "brand_name", "left")
        .join(category_dim, "category_name", "left")
        .withColumn("store_id", F.lit(1001))
        # .drop("ingestion_ts")
        # .limit(100)
    )

    candidate_pairs = canonical_df.select("brand_id", "category_id").distinct()
    filtered = woolies.join(candidate_pairs, ["brand_id", "category_id"], "inner")

    # Prepare canonical candidates for each block
    canonical_candidates = (canonical_df
    .withColumnRenamed('canonical_name','canonical_name_candidate')
    .withColumnRenamed('canonical_product_id','canonical_product_id_candidate')
    .select(
        "brand_id", "category_id", "canonical_product_id_candidate", "canonical_name_candidate"
    ))

    # Join to get all canonical candidates for each block
    enriched = filtered.join(
        canonical_candidates,
        ["brand_id", "category_id"],
        "left"
    )

    def fuzzy_match_block(pdf: pd.DataFrame) -> pd.DataFrame:
        # For each row, find best fuzzy match among canonical_name candidates
        results = []
        for idx, row in pdf.iterrows():
            name = row["canonical_name"]
            candidates = pdf["canonical_name_candidate"].tolist()
            candidate_ids = pdf["canonical_product_id_candidate"].tolist()
            lookup = dict(zip(candidates, candidate_ids))
            if not isinstance(name, str) or not name.strip():
                results.append((None, 0.0))
                continue
            result = process.extractOne(
                name,
                candidates,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=MATCH_SCORE_THRESHOLD,
            )
            if not result:
                results.append((None, 0.0))
            else:
                match_name, score, _ = result
                canonical_id = lookup.get(match_name)
                results.append((canonical_id, float(score if canonical_id is not None else 0.0)))
        pdf["canonical_product_id"] = [r[0] for r in results]
        pdf["match_score"] = [r[1] for r in results]
        pdf["match_status"] = 'matched'
        return pdf[expected_cols]

    # Use GROUPED_MAP Pandas UDF for fuzzy matching within each block
    matched = (enriched.groupby(
        "brand_id", "category_id"
    ).applyInPandas(
        fuzzy_match_block,
        schema=schema
    ).select(*store_col)
    .dropDuplicates(['store_id', 'store_sku'])
    )
    return matched



canonical_cols = [
    "canonical_name", "brand_id", "category_id", "unit_size", "unit_uom", "canonical_product_id"
]



@dp.view(name="woolworths_store_product_stage")
def woolworths_store_product_stage():
    matched = spark.read.table("woolworths_match_stage")
    return (
        matched
        .filter((F.col("canonical_product_id").isNotNull()) &
                (F.col("match_score") >= MATCH_SCORE_THRESHOLD))
        .select(
            "store_id",
            "store_sku",
            "canonical_product_id",
            "match_score",
            "match_status",
            "canonical_name"
        )
    )

@dp.table(name="02_silver.woolworths_store_product_matches")
def woolworths_store_product_matches():
    matched = spark.read.table("woolworths_match_stage")
    return (
        matched
        .filter((F.col("canonical_product_id").isNotNull()))
        .select(
            "store_id",
            "store_sku",
            "canonical_product_id",
            "match_score",
            "match_status",
            "canonical_name"
        )
    )

@dp.view(name="woolworths_canonical_payload")
def woolworths_canonical_payload():
    matched = spark.read.table("woolworths_match_stage")

    unmatched = matched.filter(F.col("canonical_product_id").isNull())

    brand_dim = spark.read.table(f"{catalog}.{silver_schema}.dim_brand").alias("db")
    category_dim = spark.read.table(f"{catalog}.{silver_schema}.dim_category").alias("dc")

    signature_cols = ["brand_id", "canonical_name", "unit_size", "unit_uom"]

    return (
        unmatched.alias("dum")
        .withColumn(
            "canonical_signature",
            F.sha2(F.concat_ws("||", *[F.col(c) for c in signature_cols]), 256)
        )
        .join(brand_dim, "brand_name", "left")
        .join(category_dim, "category_name", "left")
        .filter(F.col("db.brand_id").isNotNull() & F.col("dc.category_id").isNotNull())
        .select(
            F.col("dum.canonical_name").alias("canonical_name"),
            F.col("db.brand_id").alias("brand_id"),
            F.col("dum.brand_name").alias("brand_name"),
            F.col("dc.category_id").alias("category_id"),
            F.col("dum.category_name").alias("category_name"),
            "unit_size",
            "unit_uom",
            "canonical_signature",
        )
    )
