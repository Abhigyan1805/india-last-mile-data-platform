select h as hour_of_day,
       case when h < 6 then 'night' when h < 11 then 'morning' when h < 16 then 'afternoon'
            when h < 21 then 'evening' else 'night' end as daypart
from generate_series(0, 23) as h
