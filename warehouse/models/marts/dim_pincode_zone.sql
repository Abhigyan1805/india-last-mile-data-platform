{# One row per pincode: centroid of its post offices that have valid coordinates.
   City = the label ingest assigned to deliveries in that pincode, else the title-cased district. #}
with offices as (
    select pincode,
           min(office_name) as office_name, min(district) as district, initcap(min(state)) as state,
           avg(latitude) as latitude, avg(longitude) as longitude
    from {{ ref('stg_pincodes') }}
    group by pincode
),
geo_city as (
    select pincode, mode() within group (order by city) as city
    from (
        select pickup_pincode as pincode, pickup_city as city from {{ ref('stg_delivery_geo') }}
        union all
        select drop_pincode, drop_city from {{ ref('stg_delivery_geo') }}
    ) p
    where pincode is not null and city is not null
    group by pincode
)
select o.pincode, o.office_name, o.district, o.state,
       coalesce(g.city, initcap(o.district)) as city,
       o.latitude, o.longitude
from offices o left join geo_city g using (pincode)
