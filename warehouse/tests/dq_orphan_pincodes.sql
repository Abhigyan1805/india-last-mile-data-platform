{# DATA-QUALITY CHECK (geo pincode missing from dim_pincode_zone). severity=warn: real data is dirty, so this reports failing rows
   (and quality.check_results records the count) instead of failing the build. #}
{{ config(severity='warn') }}
select delivery_key, source, order_id, dq_flags from {{ ref('int_deliveries') }} where orphan_pincode
