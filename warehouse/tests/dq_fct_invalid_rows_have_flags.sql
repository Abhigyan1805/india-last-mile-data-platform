{# Consistency (hard failure): a row is invalid exactly when a hard flag is present. #}
select delivery_key from {{ ref('fct_deliveries') }}
where is_valid = (dq_flags && array['coords_outside_india','negative_or_absurd_delivery_time','duplicate_order_id','unparseable_timestamp'])
