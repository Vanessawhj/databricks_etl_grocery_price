CREATE MATERIALIZED VIEW 03_gold.promo_by_category AS

with grouped as (
select * 
  , if(woolworths_sku <> 0 and coalesce(woolworths_is_on_special, false) = true, woolworths_sku, null) as woolworths_special
  , if(coles_sku <> 0 and coalesce(coles_is_on_special, false) = true, coles_sku, null) as coles_special
from 03_gold.price_trend
)
select week_num, scrape_date, category_name
-- , brand_name
  , round(count(distinct woolworths_special) / count(distinct woolworths_sku), 2) * 100 woolies_promo_proportion
  , round(count(distinct coles_special) / count(distinct coles_sku),2) * 100 coles_promo_proportion
from grouped
group by week_num, scrape_date, category_name
-- , brand_name