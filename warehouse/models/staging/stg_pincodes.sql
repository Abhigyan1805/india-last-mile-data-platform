{# India Post office list: one row per post office. Coordinates outside India's bbox (or 'NA') become NULL. #}
with typed as (
    select
        trim(pincode)                                       as pincode,
        {{ clean_text('officename') }}                      as office_name,
        {{ clean_text('district') }}                        as district,
        {{ clean_text('statename') }}                       as state,
        {{ to_num('latitude') }}::double precision          as latitude,
        {{ to_num('longitude') }}::double precision         as longitude
    from {{ source('raw', 'pincodes') }}
)
select
    pincode, office_name, district, state,
    case when {{ in_india_bbox('latitude', 'longitude') }} then latitude end  as latitude,
    case when {{ in_india_bbox('latitude', 'longitude') }} then longitude end as longitude
from typed
where pincode ~ '^[1-9][0-9]{5}$'
