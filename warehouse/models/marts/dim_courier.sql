{# Zomato couriers only (Amazon has no courier id). home_city_code = letters before the first digit of the id, e.g. DEHRES. #}
select courier_id,
       regexp_replace(courier_id, '[0-9].*$', '')  as home_city_code,
       avg(courier_rating)                         as avg_rating,
       min(order_date)                             as first_seen,
       max(order_date)                             as last_seen,
       count(*)                                    as deliveries
from {{ ref('int_deliveries') }}
where courier_id is not null and not is_duplicate
group by courier_id
