{# Fact table, grain = one delivery. INCREMENTAL: on re-runs only orders from the last few days before the newest
   loaded order_date are recomputed and MERGEd on delivery_key (late arrivals update in place, nothing doubles).
   is_valid = none of the hard DQ flags fired. Invalid rows stay in the table so Spark/quality can see and count them. #}
{{ config(
    materialized='incremental',
    unique_key='delivery_key',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}
select
    d.delivery_key, d.source, d.order_id, d.courier_id, d.city,
    d.pickup_pincode, d.drop_pincode,
    d.order_ts, d.pickup_ts, d.order_date, d.order_hour,
    d.delivery_minutes, d.distance_km,
    d.weather, d.traffic, d.vehicle_type, d.area_type, d.festival, d.multiple_deliveries, d.courier_rating,
    w.rain_mm,
    not (d.coords_outside_india or d.bad_delivery_time or d.is_duplicate or d.unparseable_timestamp) as is_valid,
    d.dq_flags,
    d._load_id
from {{ ref('int_deliveries') }} d
left join {{ ref('dim_weather_daily') }} w on w.city = d.city and w.date = d.order_date
{% if is_incremental() %}
where d.order_date is null
   or d.order_date >= (select max(order_date) from {{ this }}) - {{ var('incremental_lookback_days') }}
{% endif %}
