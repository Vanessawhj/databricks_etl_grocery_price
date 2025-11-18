from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = "workspace"
silver_schema = "02_silver"
target_dim = f"{catalog}.{silver_schema}.dim_brand"


def _brand_stream(table_name: str):
    """Return a cleaned streaming view of brand names from a staging table."""
    return (
        spark.read
        .table(table_name)
        .where(F.col('__END_AT').isNull())
        .select(
            F.col("brand_name"),
            F.col("ingestion_ts"),
        )
        .where(F.col("brand_name").isNotNull())
        .withColumn("brand_name", F.trim(F.col("brand_name")))
        .where(F.col("brand_name") != "")
        .withColumn("brand_key", F.lower(F.col("brand_name")))
        .dropDuplicates(["brand_name"])   # keep one per category_name
    )


@dp.table(
    name=f"{catalog}.{silver_schema}.dim_brand",
    comment="Batch staging table for downstream processing",
    schema="""
        brand_id BIGINT GENERATED ALWAYS AS IDENTITY (start with 1 increment by 1),
        brand_name STRING,
        brand_key STRING,
        ingestion_ts TIMESTAMP
    """
)
def stage_dim_brand():
    """Streaming union of Coles + Woolworths brand names (deduped)."""
    coles = _brand_stream(f"{catalog}.{silver_schema}.coles_product_stage")
    woolies = _brand_stream(f"{catalog}.{silver_schema}.woolworths_product_stage")

    return (
        coles.unionByName(woolies, allowMissingColumns=True)
        .dropDuplicates(["brand_key"])
        .select(
            F.col("brand_name"),
            F.col("brand_key"),
            F.col("ingestion_ts"),
        )
    )
