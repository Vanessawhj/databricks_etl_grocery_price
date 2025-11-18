CREATE MATERIALIZED VIEW 03_gold.canonical_price_comparison AS
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
  inner join 02_silver.coles_product_stage c
    on sp.store_sku = c.store_sku
    and p.raw_hash = c.raw_hash
    union
  -- woolies current price
  SELECT
    s.canonical_product_id,
    s.canonical_name,
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
  inner join 02_silver.woolworths_product_stage w
    on sp.store_sku = w.store_sku
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
  -- SKU per store
  MAX(CASE WHEN j.store_id = 1000 THEN j.store_sku END) AS coles_sku,
  MAX(CASE WHEN j.store_id = 1001 THEN j.store_sku END) AS woolworths_sku,
  -- Promo prices per store
  MAX(CASE WHEN j.store_id = 1000 THEN j.promo_price END) AS coles_promo_price,
  MAX(CASE WHEN j.store_id = 1001 THEN j.promo_price END) AS woolworths_promo_price,
  -- Regular prices per store
  MAX(CASE WHEN j.store_id = 1000 THEN if(j.regular_price=0, j.promo_price, j.regular_price) END) AS coles_regular_price,
  MAX(CASE WHEN j.store_id = 1001 THEN j.regular_price END) AS woolworths_regular_price,
  -- Is on special flag
  MAX(CASE WHEN j.store_id = 1000 THEN j.is_on_special END) AS coles_is_on_special,
  MAX(CASE WHEN j.store_id = 1001 THEN j.is_on_special END) AS woolworths_is_on_special
FROM joined j
left join canonicalname c
  on j.canonical_product_id = c.canonical_product_id
where c.store_product_name is not null
GROUP BY 
  j.canonical_product_id,
  j.canonical_name,
  c.brand_name,
  c.category_name,
  c.store_product_name
ORDER BY canonical_product_id;

