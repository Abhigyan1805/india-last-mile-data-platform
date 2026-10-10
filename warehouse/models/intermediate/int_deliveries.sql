{# Unify Amazon + Zomato into one shape, attach geo/pincodes, compute distance and data-quality flags.
   Duplicates are KEPT here (flagged), because quality checks must be able to count them.
   Hard flags (make is_valid false): coords_outside_india, negative_or_absurd_delivery_time,
   duplicate_order_id, unparseable_timestamp. Soft flags (informational): null_required_field,
   pincode_unmatched, orphan_pincode. #}
with unioned as (
    select 'amazon' as source, order_id, null::text as courier_id,
           pickup_lat, pickup_lon, drop_lat, drop_lon, order_date, order_time, pickup_time,
           weather, traffic, vehicle_type, area_type, null::boolean as festival, null::int as multiple_deliveries,
           courier_rating, delivery_minutes, _load_id
    from {{ ref('stg_amazon_deliveries') }}
    union all
    select 'zomato', order_id, courier_id,
           pickup_lat, pickup_lon, drop_lat, drop_lon, order_date, order_time, pickup_time,
           weather, traffic, vehicle_type, area_type, festival, multiple_deliveries,
           courier_rating, delivery_minutes, _load_id
    from {{ ref('stg_zomato_deliveries') }}
),
joined as (
    select u.*,
           (u.order_date + u.order_time)                                   as order_ts,
           -- pickup after midnight relative to order time -> next day
           case when u.pickup_time is not null and u.order_date is not null then
                u.order_date + u.pickup_time + case when u.pickup_time < u.order_time then interval '1 day' else interval '0' end
           end                                                             as pickup_ts,
           g.pickup_city                                                   as city,
           g.pickup_pincode as pickup_pincode_geo, g.drop_pincode as drop_pincode_geo,
           coalesce(g.pickup_matched, false) as pickup_matched, coalesce(g.drop_matched, false) as drop_matched,
           row_number() over (partition by u.source, u.order_id
                              order by (u.order_date + u.order_time) nulls last, u.delivery_minutes, u.pickup_lat) as dup_rank
    from unioned u
    left join {{ ref('stg_delivery_geo') }} g on g.source = u.source and g.order_id = u.order_id
),
checked as (
    select j.*,
           {{ in_india_bbox('pickup_lat', 'pickup_lon') }} and {{ in_india_bbox('drop_lat', 'drop_lon') }} as coords_ok,
           (delivery_minutes <= 0 or delivery_minutes > {{ var('max_delivery_minutes') }})                  as bad_time,
           (order_ts is null)                                                                               as bad_ts,
           (pickup_pincode_geo is not null and pickup_matched and pz1.pincode is null)
             or (drop_pincode_geo is not null and drop_matched and pz2.pincode is null)                     as orphan,
           pz1.pincode as pickup_pincode, pz2.pincode as drop_pincode
    from joined j
    left join {{ ref('dim_pincode_zone') }} pz1 on pz1.pincode = case when j.pickup_matched then j.pickup_pincode_geo end
    left join {{ ref('dim_pincode_zone') }} pz2 on pz2.pincode = case when j.drop_matched then j.drop_pincode_geo end
)
select
    source || '-' || order_id || case when dup_rank > 1 then '#' || dup_rank else '' end as delivery_key,
    source, order_id, courier_id, city,
    pickup_pincode, drop_pincode,
    order_ts, pickup_ts, order_date, extract(hour from order_ts)::smallint as order_hour,
    delivery_minutes,
    -- distance only for rows with sane coordinates (garbage in, NULL out)
    case when coords_ok then {{ haversine_km('pickup_lat', 'pickup_lon', 'drop_lat', 'drop_lon') }} end as distance_km,
    weather, traffic, vehicle_type, area_type, festival, multiple_deliveries, courier_rating,
    pickup_lat, pickup_lon, drop_lat, drop_lon,
    dup_rank > 1                                                    as is_duplicate,
    not coords_ok                                                   as coords_outside_india,
    coalesce(bad_time, false)                                       as bad_delivery_time,
    bad_ts                                                          as unparseable_timestamp,
    (delivery_minutes is null)                                      as missing_delivery_time,
    not (pickup_matched and drop_matched)                           as pincode_unmatched,
    coalesce(orphan, false)                                         as orphan_pincode,
    _load_id,
    array_remove(array[
        case when not coords_ok then 'coords_outside_india' end,
        case when bad_time then 'negative_or_absurd_delivery_time' end,
        case when dup_rank > 1 then 'duplicate_order_id' end,
        case when bad_ts then 'unparseable_timestamp' end,
        case when delivery_minutes is null then 'null_required_field' end,
        case when not (pickup_matched and drop_matched) then 'pincode_unmatched' end,
        case when orphan then 'orphan_pincode' end
    ], null)                                                        as dq_flags
from checked
where order_id is not null
