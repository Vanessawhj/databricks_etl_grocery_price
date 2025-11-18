"""Pricing observation pipeline: normalize Coles/Woolworths price feeds and enrich with store_product."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window

catalog = "workspace"
bronze_schema = "01_bronze"
silver_schema = "02_silver"
target_table = f"{catalog}.{silver_schema}.price_observation"

SRC_COLS = [
    'store_sku',
    'unit_price',
    'unit_measure',
    'regular_price',
    'promo_price',
    'promo_details',
    'raw_hash',
    'scrape_timestamp',
    'in_stock',
    'is_on_special',
    'is_half_price',
    # 'source_raw_pk',
    'batch_ts',
]

@dp.view(name="price_observation_stage")
@dp.expect_or_drop("known_store_ids", "store_id IN (1000, 1001)")
@dp.expect_or_drop("scrape_ts_present", "scrape_timestamp IS NOT NULL")
@dp.expect_or_drop("regular_price_non_negative", "regular_price >= 0")
@dp.expect_or_drop("promo_price_non_negative", "promo_price >= 0")
@dp.expect_or_drop("has_store_product_match", "store_product_id IS NOT NULL")
def price_observation_stage():
    """Normalize promo/regular pricing and enrich with store_product/canonical IDs."""
    coles = (
        spark.readStream.table(f"{catalog}.{bronze_schema}.coles_product_raw")
             .withColumn(
                 "regular_price",
                 F.when(F.col("price_was") == 0, F.col("price_now")).otherwise(F.col("price_was"))
             )
             .withColumnRenamed("price_now", "promo_price")
             .withColumnRenamed("availability", "in_stock")
             .withColumnRenamed("promotionType", "promotion_type")
             .withColumnRenamed("specialType", "special_type")
             .withColumnRenamed("offerDescription", "offer_description")
             .withColumnRenamed("id", "store_sku")
             .withColumn("promo_details", F.lit(""))
             .withColumn("is_on_special", F.lit(None).cast("boolean"))
             .withColumn("is_half_price", F.lit(None).cast("boolean"))
             .withColumn("in_stock", F.lit(None).cast("boolean"))
             .select(SRC_COLS + ["promotion_type", "special_type", "offer_description"])
             .withColumn("store_id", F.lit(1000))
    )
    promo_mask = (
        F.col("promotion_type").isNotNull()
        | F.col("special_type").isNotNull()
        | F.col("offer_description").isNotNull()
    )
    promo_cols = F.concat_ws(
        "|",
        F.coalesce(F.col("promotion_type"), F.lit("")),
        F.coalesce(F.col("special_type"), F.lit("")),
        F.coalesce(F.col("offer_description"), F.lit("")),
    )
    coles = (
        coles
        .withColumn("promo_details", F.when(promo_mask, promo_cols).otherwise(F.col("promo_details")))
        .withColumn("is_on_special", F.when(promo_mask, F.lit(True)).otherwise(F.col("is_on_special")))
        .drop("promotion_type", "special_type", "offer_description")
    )

    woolies = (
        spark.readStream.table(f"{catalog}.{bronze_schema}.woolworths_product_raw")
             .withColumnRenamed("WasPrice", "regular_price")
             .withColumnRenamed("Price", "promo_price")
             .withColumnRenamed("Stockcode", "store_sku")
             .withColumnRenamed("unit_uom", "unit_measure")
             .withColumnRenamed("InStock", "in_stock")
             .withColumnRenamed("IsOnSpecial", "is_on_special")
             .withColumnRenamed("IsHalfPrice", "is_half_price")
             .withColumn("is_on_special", F.col('is_on_special').cast("boolean"))
             .withColumn("is_half_price", F.col('is_half_price').cast("boolean"))
             .withColumn("in_stock", F.col('in_stock').cast("boolean"))
             .withColumn("promo_details", F.lit(""))
             .withColumn("offer_description", F.lit(""))
             .withColumn("special_type", F.lit(""))
             .withColumn("promotion_type", F.lit(""))
             .select(SRC_COLS)
             .withColumn("store_id", F.lit(1001))
    )


    column_order = [
        'store_sku', 'unit_price', 'unit_measure', 'regular_price', 'promo_price', 'promo_details', 
        'raw_hash', 'scrape_timestamp', 
        'in_stock', 'is_on_special', 'batch_ts', 
         'is_half_price', 
             'store_id'
                    ]

    coles = coles.withColumn("source_store", F.lit("Coles")).select(*column_order)
    woolies = woolies.withColumn("source_store", F.lit("Woolworths")).select(*column_order)
    staged = coles.unionByName(woolies)

    store_products = spark.read.table(f"{catalog}.{silver_schema}.store_product") \
        .select("store_product_id", "store_id", "store_sku", "canonical_product_id")

    enriched = (
        staged.join(store_products, ["store_id", "store_sku"], "left")
              .filter(F.col("regular_price").isNotNull())
              .filter(F.col("store_product_id").isNotNull())
              .filter(F.col("scrape_timestamp").isNotNull())
              .select( 
                  "store_product_id", "canonical_product_id", "store_id", "scrape_timestamp",
                  "regular_price", "promo_price", "promo_details", "unit_price",
                  "unit_measure", "raw_hash", "in_stock", "is_on_special", "is_half_price",
                  "batch_ts"
              )
              .dropDuplicates(["store_product_id", "scrape_timestamp"])
    )


    return enriched

dp.create_streaming_table(
    name=target_table,
    schema="""
        observation_id BIGINT GENERATED ALWAYS AS IDENTITY,
        store_product_id BIGINT,
        canonical_product_id BIGINT,
        store_id INT,
        scrape_timestamp TIMESTAMP,
        regular_price DOUBLE,
        promo_price DOUBLE,
        promo_details STRING,
        unit_price DOUBLE,
        unit_measure STRING,
        raw_hash STRING,
        in_stock BOOLEAN,
        is_on_special BOOLEAN,
        is_half_price BOOLEAN,
        batch_ts TIMESTAMP
    """,
    table_properties={"pipelines.autoOptimize.managed": "true"},
    comment="Price observations for Coles & Woolworths"
)


dp.create_auto_cdc_flow(
    target=target_table,
    source="price_observation_stage",
    keys=["store_product_id", "scrape_timestamp"],
    sequence_by="batch_ts",
    stored_as_scd_type=1,
    column_list=[
        "store_product_id", "canonical_product_id", "store_id", "scrape_timestamp",
        "regular_price", "promo_price", "promo_details", "unit_price", "unit_measure",
        "raw_hash", "in_stock", "is_on_special", "is_half_price", "batch_ts"
    ],
)
