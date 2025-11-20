"""Bridge store SKUs to canonical products."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window

catalog = "workspace"
silver_schema = "02_silver"
target_table = f"{catalog}.{silver_schema}.store_product"

store_product_cols = ['canonical_name', 'store_id', 'store_sku', 'canonical_product_id', 'match_score', 'match_status']
 
@dp.table(
    name=target_table,
    schema="""
        store_product_id BIGINT GENERATED ALWAYS AS IDENTITY (start with 1 increment by 1),
        store_id INT,
        store_sku STRING,
        canonical_product_id BIGINT,
        match_score DOUBLE,
        match_status STRING,
        canonical_name STRING
    """,
    table_properties={"pipelines.autoOptimize.managed": "true"},
    comment="Store-to-canonical product matches",
)
def store_product_stage():
    """Union Coles bootstrap matches with Woolworths fuzzy matches to build the store_product bridge."""
    stage = (
        spark.read
            .table(f"{catalog}.{silver_schema}.coles_product_stage")
             .select("store_id", "store_sku", "canonical_name")
             .where(F.col('__END_AT').isNull())

    )
    canonical = spark.read.table(f"{catalog}.{silver_schema}.canonical") \
        .select("canonical_product_id", "canonical_name")

    coles_stage = (
        stage.join(canonical, "canonical_name", "left")
             .withColumn("match_score", F.lit(100.0))
             .withColumn("match_status", F.lit("bootstrap"))
    )
    
    coles = coles_stage.select(*store_product_cols)

    woolies = (
        spark.read
        .table("woolworths_store_product_stage")
        .select(*store_product_cols)
    )

    staged = coles.unionByName(woolies)
    
    return staged
