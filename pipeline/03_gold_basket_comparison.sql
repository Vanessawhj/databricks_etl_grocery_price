CREATE MATERIALIZED VIEW 03_gold.basket_comparison AS

with categories as (
    select 
        case 
            when canonical_name like '%milk%' then 'Milk'
            when canonical_name like '%egg%' then 'Egg'
            when canonical_name like '%tim%tam%' then 'Snacks'
            when canonical_name like '%butter%' then 'Butter'
            when canonical_name like '%pasta%' then 'Pasta'
            when category_name = 'freezer' then 'Ice cream'
            when category_name = 'international_food' then 'International food'
            end category
    , coalesce(woolworths_regular_price, 0) woolworths_regular_price
    , coalesce(woolworths_promo_price, 0) woolworths_promo_price
    , 1-coalesce(woolworths_promo_price, 0)/coalesce(woolworths_regular_price, 0) woolworths_promo
    , coalesce(coles_regular_price, 0) coles_regular_price
    , coalesce(coles_promo_price, 0) coles_promo_price
    , 1-coalesce(coles_promo_price, 0)/coalesce(coles_regular_price, 0) coles_promo
    from 03_gold.canonical_price_comparison
    where 1=1
        and 
        (
            (canonical_name like '%milk%' and category_name = 'dairy_eggs_fridge'and brand_name in ('Pauls','Vitasoy','Devondale'))
            or (canonical_name like '%egg%' and category_name = 'dairy_eggs_fridge' and brand_name in ('Sunny Queen'))
            or (canonical_name like '%tim%tam%'  and category_name = 'snacks_confectionary' and brand_name in ('Arnotts'))
            or (canonical_name like '%butter%' and category_name = 'dairy_eggs_fridge' and brand_name in ('Devondale', 'Lurpak'))
            or (canonical_name like '%pasta%' and category_name = 'dairy_eggs_fridge' and brand_name in ('25 Degrees South'))
            or (category_name = 'freezer' and brand_name in ('Connoisseur','Sara Lee',' Peters','Reese''s'))
            or (category_name = 'international_food')
        )
)
select category
    , avg(woolworths_regular_price) avg_woolworths_regular_price
    , avg(woolworths_promo_price) avg_woolworths_promo_price
    , round(avg(woolworths_promo)*100, 2) avg_woolworths_promo
    , avg(coles_regular_price) avg_coles_regular_price
    , avg(coles_promo_price) avg_coles_promo_price
    , round(avg(coles_promo)*100,2) avg_coles_promo
    , if(avg(woolworths_regular_price) > avg(coles_regular_price), 'Coles','Woolworths') cheaper_retailer
from categories
group by category