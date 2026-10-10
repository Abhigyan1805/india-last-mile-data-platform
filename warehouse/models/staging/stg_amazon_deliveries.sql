{# Amazon India delivery records: cast text -> typed, trim categoricals, NaN -> NULL. One row per raw row (duplicates kept on purpose). #}
select
    {{ clean_text('order_id') }}                                    as order_id,
    {{ to_num('agent_age') }}                                       as courier_age,
    {{ to_num('agent_rating') }}                                    as courier_rating,
    {{ to_num('store_latitude') }}::double precision                as pickup_lat,
    {{ to_num('store_longitude') }}::double precision               as pickup_lon,
    {{ to_num('drop_latitude') }}::double precision                 as drop_lat,
    {{ to_num('drop_longitude') }}::double precision                as drop_lon,
    {{ parse_date_iso('order_date') }}                              as order_date,
    {{ parse_time('order_time') }}                                  as order_time,
    {{ parse_time('pickup_time') }}                                 as pickup_time,
    lower({{ clean_text('weather') }})                              as weather,
    lower({{ clean_text('traffic') }})                              as traffic,
    lower({{ clean_text('vehicle') }})                              as vehicle_type,
    lower({{ clean_text('area') }})                                 as area_type,
    {{ to_num('delivery_time') }}                                   as delivery_minutes,
    {{ clean_text('category') }}                                    as category,
    _load_id
from {{ source('raw', 'amazon_deliveries') }}
