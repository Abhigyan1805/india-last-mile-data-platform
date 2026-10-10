{# Calendar from the first to the last order date (padded a month each side) with a festival-season flag from the festival_windows seed. #}
with bounds as (
    select coalesce(min(order_date), current_date) - 31 as d0, coalesce(max(order_date), current_date) + 31 as d1
    from {{ ref('int_deliveries') }}
)
select d::date                                   as date_key,
       extract(year from d)::int                 as year,
       extract(month from d)::int                as month,
       extract(day from d)::int                  as day,
       extract(isodow from d)::int               as dow,
       extract(isodow from d) in (6, 7)          as is_weekend,
       exists (select 1 from {{ ref('festival_windows') }} f
               where d::date between f.start_date::date and f.end_date::date) as is_festival_season
from bounds, generate_series(d0, d1, interval '1 day') as d
