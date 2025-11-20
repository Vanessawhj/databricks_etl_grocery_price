from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = "workspace"
silver_schema = "02_silver"
target_dim = f"{catalog}.{silver_schema}.dim_category"


def _category_stream(table_name: str):
    """Return a cleaned streaming view of category names from a staging table."""
    return (
        spark.read
        .table(table_name)
        .where(F.col('__END_AT').isNull())
        .select(
            F.col("category_name"),
            F.col("ingestion_ts"),
        )
        .where(F.col("category_name").isNotNull())
        .where(F.col("category_name") != "")
        .withWatermark("ingestion_ts", "7 days")
        .dropDuplicates(["category_name"]
    )
    )


@dp.table(
    name=f"{catalog}.{silver_schema}.dim_category",
    schema="""
        category_id BIGINT GENERATED ALWAYS AS IDENTITY  (start with 1 increment by 1),
        category_name STRING,
        ingestion_ts TIMESTAMP
    """,
    comment="Unique list of categorys sourced from stage tables."
)
def stage_dim_category():
    """Streaming union of Coles + Woolworths category names (deduped)."""
    coles = _category_stream(f"{catalog}.{silver_schema}.coles_product_stage")
    woolies = _category_stream(f"{catalog}.{silver_schema}.woolworths_product_stage")

    return (
        coles.unionByName(woolies, allowMissingColumns=True)
        .dropDuplicates(["category_name"])
        .select(
            F.col("category_name"),
            F.col("ingestion_ts"),
        )
    )
