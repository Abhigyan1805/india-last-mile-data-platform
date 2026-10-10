select city, date, count(*) from {{ ref('dim_weather_daily') }} group by 1, 2 having count(*) > 1
