{# DATA-QUALITY CHECK (delivery time <= 0 or > max_delivery_minutes). severity=warn: real data is dirty, so this reports failing rows
   (and quality.check_results records the count) instead of failing the build. #}
{{ config(severity='warn') }}
select delivery_key, source, order_id, dq_flags from {{ ref('int_deliveries') }} where bad_delivery_time
