{# Daily weather per city. rain_bucket thresholds (mm/day) are a simplification of IMD classes: none <0.1, light <2.5, moderate <7.6, heavy otherwise. #}
select city, weather_date as date, temp_max_c, temp_min_c, precipitation_mm, rain_mm, wind_max_kmh,
       case when rain_mm is null then null
            when rain_mm < 0.1 then 'none' when rain_mm < 2.5 then 'light'
            when rain_mm < 7.6 then 'moderate' else 'heavy' end as rain_bucket
from {{ ref('stg_weather_daily') }}
