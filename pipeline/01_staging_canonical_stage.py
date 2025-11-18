from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window

catalog = "workspace"
silver_schema = "02_silver"
target_table = f"{catalog}.{silver_schema}.canonical_stage"


@dp.table()
def coles_canonical():
    stage = (spark.read
            #  .option("skipChangeCommits", "true")
             .table(f"{catalog}.{silver_schema}.coles_product_stage")
            .withWatermark("ingestion_ts", "1 hour")
            .where(F.col('__END_AT').isNull())
            .alias('stg'))
    dim_brand = (spark.read
                 .table(f"{catalog}.{silver_schema}.dim_brand")
                # .withWatermark("ingestion_ts", "1 hour")
                 )
    dim_category = (spark.read
                    .table(f"{catalog}.{silver_schema}.dim_category")
                    # .withWatermark("ingestion_ts", "1 hour")
                    )

    stage = stage.dropDuplicates(["store_id", "store_sku"])
    stage = (stage
             .join(dim_brand, "brand_name", "left")
             .join(dim_category, "category_name", "left")
             .withColumn(
                 "canonical_signature",
                 F.sha2(
                     F.concat_ws(
                         "||",
                         F.col("brand_id"),
                         F.col("canonical_name"),
                         F.col("unit_size"),
                         F.col("unit_uom"),
                     ),
                     256,
                 ),
             )
             
            )
    stage = stage.dropDuplicates(["store_sku"])
    w = Window.orderBy(F.col("canonical_signature"))

    stage = (
        stage.withColumn("canonical_product_id", F.row_number().over(w))
    )


    return (
        stage.select( 
                "canonical_product_id",
                 "canonical_name",
                 "brand_id",
                 "brand_name",
                 "category_id",
                 "category_name",
                 "unit_size",
                 "unit_uom",
                 "canonical_signature",
                 F.col("stg.ingestion_ts")
             )
    )
