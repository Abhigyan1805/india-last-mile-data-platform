{# Nearest-pincode enrichment from ingest/geo.py, pivoted to one row per order with pickup_* and drop_* columns.
   Only the latest _load_id per source is used (ingest keeps older runs out, this is belt and braces). #}
with latest as (
    select * from {{ source('raw', 'delivery_geo') }} g
    where _load_id = (select max(_load_id) from {{ source('raw', 'delivery_geo') }} where source = g.source)
)
select
    source, order_id,
    max(case when point_kind = 'pickup' then pincode end)  as pickup_pincode,
    bool_or(point_kind = 'pickup' and matched)             as pickup_matched,
    max(case when point_kind = 'pickup' then city end)     as pickup_city,
    max(case when point_kind = 'drop' then pincode end)    as drop_pincode,
    bool_or(point_kind = 'drop' and matched)               as drop_matched,
    max(case when point_kind = 'drop' then city end)       as drop_city
from latest
group by source, order_id
