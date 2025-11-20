from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window, types as T

catalog = "workspace"
silver_schema = "02_silver"
target_table = f"{catalog}.{silver_schema}.canonical"
canonical_cols = ["canonical_product_id", "canonical_name", "brand_id", "brand_name", "category_id", "category_name", "unit_size", "unit_uom", "canonical_signature", "ingestion_ts"]
                       
        
@dp.table(
    name=target_table,
    schema="""
        canonical_product_id BIGINT,
        canonical_name STRING,
        brand_id BIGINT,
        brand_name STRING,
        category_id BIGINT,
        category_name STRING,
        unit_size DOUBLE,
        unit_uom STRING,
        canonical_signature STRING,
        ingestion_ts TIMESTAMP
    """,
    table_properties={"pipelines.autoOptimize.managed": "true",
                      "delta.enableChangeDataFeed": "true"},
    comment="Canonical product dimension",
)
def canonical():
    coles = (
            spark.read
             .table("coles_canonical")
            .withWatermark("ingestion_ts", "1 hour")
            .select(*canonical_cols)
            )
    
    matched = spark.read.table("woolworths_match_stage")

    unmatched = matched.filter(F.col("canonical_product_id").isNull())

    dim_brand = (spark.read
                 .table(f"{catalog}.{silver_schema}.dim_brand")
                .drop('ingestion_ts')
                .alias('db')
                 )
    dim_category = (spark.read
                    .table(f"{catalog}.{silver_schema}.dim_category")
                    .drop('ingestion_ts')
                    ).alias('dc')

    signature_cols = ["brand_id", "canonical_name", "unit_size", "unit_uom"]


    prev_max = spark.read.table("coles_canonical").agg(F.max("canonical_product_id")).first()[0] or 27624

    w = Window.orderBy(F.col("canonical_signature"))

    woolies = (
        unmatched.alias("dum")
        .withColumn(
            "canonical_signature",
            F.sha2(F.concat_ws("||", *[F.col(c) for c in signature_cols]), 256)
        )
        .join(dim_brand, "brand_name", "left")
        .join(dim_category, "category_name", "left")
        .filter(F.col("db.brand_id").isNotNull() & F.col("dc.category_id").isNotNull())
        .withColumn("canonical_product_id", (F.row_number().over(w) + F.lit(prev_max)).cast(T.LongType()))
        .select(
            "canonical_product_id",
            F.col("dum.canonical_name").alias("canonical_name"),
            F.col("db.brand_id").alias("brand_id"),
            F.col("dum.brand_name").alias("brand_name"),
            F.col("dc.category_id").alias("category_id"),
            F.col("dum.category_name").alias("category_name"),
            "unit_size",
            "unit_uom",
            "canonical_signature",
            'ingestion_ts'
        )
    )
    
    staged = coles.unionByName(woolies)
    
    return staged
