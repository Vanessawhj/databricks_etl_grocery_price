CREATE MATERIALIZED VIEW 03_gold.woolworths_price_trend AS

select sp.store_product_id, sp.canonical_product_id, sp.store_id, s.category_name , s.brand_name , sp.canonical_name, r.name, p.regular_price, coalesce(p.promo_price, 0) promo_price, p.promo_details, p.is_on_special, p.unit_price, p.unit_measure, date(p.scrape_timestamp), r.raw_hash
from 01_bronze.woolworths_product_raw r
left join (select distinct store_sku, raw_hash, category_name, brand_name from 02_silver.woolworths_product_stage where `__END_AT` is null) s
  on r.Stockcode = s.store_sku
inner join 02_silver.store_product sp
  on r.Stockcode = sp.store_sku
  and sp.store_id = 1001
inner join 02_silver.price_observation p
  on sp.store_product_id = p.store_product_id
  and p.raw_hash = r.raw_hash
