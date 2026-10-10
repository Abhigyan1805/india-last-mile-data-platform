{# Zomato delivery records. dates are dd-mm-yyyy, Time_taken looks like "(min) 24", categoricals have trailing spaces. #}
select
    {{ clean_text('id') }}                                          as order_id,
    {{ clean_text('delivery_person_id') }}                          as courier_id,
    {{ to_num('delivery_person_age') }}                             as courier_age,
    {{ to_num('delivery_person_ratings') }}                         as courier_rating,
    {{ to_num('restaurant_latitude') }}::double precision           as pickup_lat,
    {{ to_num('restaurant_longitude') }}::double precision          as pickup_lon,
    {{ to_num('delivery_location_latitude') }}::double precision    as drop_lat,
    {{ to_num('delivery_location_longitude') }}::double precision   as drop_lon,
    {{ parse_date_dmy('order_date') }}                              as order_date,
    {{ parse_time('time_orderd') }}                                 as order_time,
    {{ parse_time('time_order_picked') }}                           as pickup_time,
    -- source values look like "conditions Sunny"
    lower(regexp_replace({{ clean_text('weather_conditions') }}, '^conditions\s+', '', 'i')) as weather,
    lower({{ clean_text('road_traffic_density') }})                 as traffic,
    {{ to_num('vehicle_condition') }}::int                          as vehicle_condition,
    {{ clean_text('type_of_order') }}                               as category,
    lower({{ clean_text('type_of_vehicle') }})                      as vehicle_type,
    {{ to_num('multiple_deliveries') }}::int                        as multiple_deliveries,
    case lower({{ clean_text('festival') }}) when 'yes' then true when 'no' then false end as festival,
    lower({{ clean_text('city') }})                                 as area_type,   -- Metropolitian/Urban/Semi-Urban, NOT a city
    {{ to_num('time_taken_min') }}                                  as delivery_minutes,
    _load_id
from {{ source('raw', 'zomato_deliveries') }}
