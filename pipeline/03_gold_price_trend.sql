CREATE MATERIALIZED VIEW 03_gold.price_trend AS

-- Find canonical products sold by more than one store

WITH similar AS (
  SELECT c.canonical_product_id, c.canonical_name
  FROM 02_silver.canonical c
  JOIN 02_silver.store_product sp
    ON c.canonical_product_id = sp.canonical_product_id
  GROUP BY c.canonical_product_id, c.canonical_name
  HAVING COUNT(DISTINCT sp.store_id) > 1
),
joined AS (
  -- coles current price
  SELECT
    s.canonical_product_id,
    s.canonical_name,
    CASE
      WHEN DATE (c.scrape_timestamp) IN ('2025-11-04','2025-11-10') THEN 1
      WHEN DATE (c.scrape_timestamp) IN ('2025-11-07','2025-11-12') THEN 2
      WHEN DATE (c.scrape_timestamp) IN ('2025-11-17') THEN 3
    END AS week_num,
    CASE
      WHEN DATE (c.scrape_timestamp) IN ('2025-11-04','2025-11-10') THEN '2025-11-03'
      WHEN DATE (c.scrape_timestamp) IN ('2025-11-07','2025-11-12') THEN '2025-11-10'
      WHEN DATE (c.scrape_timestamp) IN ('2025-11-17') THEN '2025-11-17'
    END AS scrape_date,
    sp.store_id,
    sp.store_sku,
    sp.store_product_id,
    p.promo_price,
    p.regular_price, 
    coalesce(p.is_on_special, false) is_on_special
  FROM similar s
  JOIN 02_silver.store_product sp
    ON s.canonical_product_id = sp.canonical_product_id
  LEFT JOIN 02_silver.price_observation p
    ON sp.store_product_id = p.store_product_id
  left join 01_bronze.coles_product_raw c
    on sp.store_sku = c.id
    and p.raw_hash = c.raw_hash
    union
  -- woolies current price
  SELECT
    s.canonical_product_id,
    s.canonical_name,
    CASE
      WHEN DATE (w.scrape_timestamp) IN ('2025-11-04','2025-11-10') THEN 1
      WHEN DATE (w.scrape_timestamp) IN ('2025-11-07','2025-11-12') THEN 2
      WHEN DATE (w.scrape_timestamp) IN ('2025-11-17') THEN 3
    END AS week_num,
    CASE
      WHEN DATE (w.scrape_timestamp) IN ('2025-11-04','2025-11-10') THEN '2025-11-03'
      WHEN DATE (w.scrape_timestamp) IN ('2025-11-07','2025-11-12') THEN '2025-11-10'
      WHEN DATE (w.scrape_timestamp) IN ('2025-11-17') THEN '2025-11-17'
    END AS scrape_date,
    sp.store_id,
    sp.store_sku,
    sp.store_product_id,
    p.promo_price,
    p.regular_price, 
    p.is_on_special
  FROM similar s
  JOIN 02_silver.store_product sp
    ON s.canonical_product_id = sp.canonical_product_id
  LEFT JOIN 02_silver.price_observation p
    ON sp.store_product_id = p.store_product_id
  left join 01_bronze.woolworths_product_raw w
    on sp.store_sku = w.Stockcode
)
, canonicalName as (
  select distinct canonical_product_id, c.store_product_name, c.brand_name, c.category_name
  from joined sp
  left join 02_silver.coles_product_stage c 
    on c.store_sku = sp.store_sku
)
-- Pivot prices by store_id; add other fields similarly if needed
SELECT distinct
  j.canonical_product_id,
  j.canonical_name,
  c.store_product_name,
  c.brand_name,
  c.category_name,
  scrape_date,
  week_num,
  -- SKU per store
  coalesce(MAX(CASE WHEN j.store_id = 1000 THEN j.store_sku END),0) AS coles_sku,
  coalesce(MAX(CASE WHEN j.store_id = 1001 THEN j.store_sku END),0) AS woolworths_sku,
  -- Promo prices per store
  coalesce(MAX(CASE WHEN j.store_id = 1000 THEN coalesce(j.promo_price,0) END),0) AS coles_promo_price,
  coalesce(MAX(CASE WHEN j.store_id = 1001 THEN coalesce(j.promo_price,0) END),0) AS woolworths_promo_price,
  -- Regular prices per store
  coalesce(MAX(CASE WHEN j.store_id = 1000 THEN if(j.regular_price=0, coalesce(j.promo_price,0), coalesce(j.regular_price,0)) END),0) AS coles_regular_price,
  coalesce(MAX(CASE WHEN j.store_id = 1001 THEN coalesce(j.regular_price,0) END),0) AS woolworths_regular_price,
  -- Promo discount
  coalesce(MAX(CASE WHEN j.store_id = 1000 THEN 1-(coalesce(j.promo_price,0)/if(j.regular_price=0, coalesce(j.promo_price,0), coalesce(j.regular_price,0))) END),0) AS coles_promo,
  coalesce(MAX(CASE WHEN j.store_id = 1001 THEN 1-(coalesce(j.promo_price,0)/coalesce(j.regular_price,0)) END),0) AS woolworths_promo,
  -- on special flag
  coalesce(MAX(CASE WHEN j.store_id = 1000 THEN coalesce(j.is_on_special,false) END)) AS coles_is_on_special,
  coalesce(MAX(CASE WHEN j.store_id = 1001 THEN coalesce(j.is_on_special,false) END)) AS woolworths_is_on_special
FROM joined j
left join canonicalname c
  on j.canonical_product_id = c.canonical_product_id
where c.store_product_name is not null
  and week_num is not null
GROUP BY 
  scrape_date,
  week_num,
  j.canonical_product_id,
  j.canonical_name,
  c.brand_name,
  c.category_name,
  c.store_product_name
ORDER BY canonical_product_id;




