{# Cities that appear on deliveries or in the city reference list. State/coords from city_ref, else derived from pincodes. #}
with seen as (
    select city, count(*) filter (where not is_duplicate) as deliveries
    from {{ ref('int_deliveries') }} where city is not null group by city
),
derived as (
    select city, mode() within group (order by state) as state, avg(latitude) as latitude, avg(longitude) as longitude
    from {{ ref('dim_pincode_zone') }} where city is not null group by city
)
select c.city,
       coalesce(r.state, d.state)         as state,
       coalesce(r.latitude, d.latitude)   as latitude,
       coalesce(r.longitude, d.longitude) as longitude,
       coalesce(s.deliveries, 0)          as deliveries
from (select city from seen union select city from {{ ref('stg_city_ref') }}) c
left join {{ ref('stg_city_ref') }} r using (city)
left join derived d using (city)
left join seen s using (city)
