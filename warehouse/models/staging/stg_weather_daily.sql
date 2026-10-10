{# Daily weather per city (Open-Meteo archive, loaded by ingest). rain_mm NULL stays NULL (unknown != dry). #}
select city, date as weather_date, temp_max_c, temp_min_c, precipitation_mm, rain_mm, wind_max_kmh, weather_code
from {{ source('raw', 'weather_daily') }}
