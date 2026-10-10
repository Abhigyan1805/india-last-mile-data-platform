{# Reusable data-quality / cleaning expressions. One definition, used by models, tests and the quality runner. #}

{# TRUE when (lat, lon) is a real coordinate inside India's bounding box. NULL inputs -> FALSE. #}
{% macro in_india_bbox(lat, lon) -%}
    coalesce({{ lat }} between {{ var('india_lat_min') }} and {{ var('india_lat_max') }}
         and {{ lon }} between {{ var('india_lon_min') }} and {{ var('india_lon_max') }}, false)
{%- endmacro %}

{# Text -> numeric. Trims, maps 'NaN'/'NA'/'' to NULL, drops anything non-numeric (e.g. "(min) 24" -> 24). #}
{% macro to_num(col) -%}
    nullif(substring(trim({{ col }}) from '-?[0-9]+(?:\.[0-9]+)?'), '')::numeric
{%- endmacro %}

{# Text -> trimmed text with 'NaN'/'NA'/'' as NULL. #}
{% macro clean_text(col) -%}
    case when upper(trim({{ col }})) in ('', 'NAN', 'NA', 'NONE', 'NULL') then null else trim({{ col }}) end
{%- endmacro %}

{# Great-circle distance in km between two points (haversine). NULL if any input is NULL. #}
{% macro haversine_km(lat1, lon1, lat2, lon2) -%}
    (2 * 6371.0088 * asin(sqrt(
        power(sin(radians(({{ lat2 }} - {{ lat1 }}) / 2)), 2)
        + cos(radians({{ lat1 }})) * cos(radians({{ lat2 }}))
          * power(sin(radians(({{ lon2 }} - {{ lon1 }}) / 2)), 2)
    )))
{%- endmacro %}

{# Safe date parsers: return NULL (never an error) on malformed text, so bad rows are counted, not fatal. #}
{% macro parse_date_iso(col) -%}
    case when trim({{ col }}) ~ '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$' then trim({{ col }})::date end
{%- endmacro %}
{% macro parse_date_dmy(col) -%}
    case when trim({{ col }}) ~ '^(0[1-9]|[12]\d|3[01])-(0[1-9]|1[0-2])-\d{4}$' then to_date(trim({{ col }}), 'DD-MM-YYYY') end
{%- endmacro %}
{# 'HH:MM[:SS]' text -> time, NULL when malformed. #}
{% macro parse_time(col) -%}
    case when trim({{ col }}) ~ '^([01]\d|2[0-3]):[0-5]\d(:[0-5]\d)?$' then trim({{ col }})::time end
{%- endmacro %}
