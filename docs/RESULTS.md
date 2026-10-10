# Results of the recorded end-to-end run

Generated 2026-10-10 12:48 by `scripts/e2e_run.sh` (native runtime, no Docker). Every number is queried from the warehouse.


## Ingest
| load_id | sample | amazon rows | zomato rows | pincode rows | weather city-days | geo points | matched | match rate % |
|---|---|---|---|---|---|---|---|---|
| 20261010T124209 | False | 43739 | 45584 | 165627 | 1320 | 178646 | 163249 | 91.38 |

## Pincode match by source / point
| source | point | points | matched | match % | avg km to pincode |
|---|---|---|---|---|---|
| amazon | drop | 43739 | 39995 | 91.44 | 2.69 |
| amazon | pickup | 43739 | 40046 | 91.56 | 1.18 |
| zomato | drop | 45584 | 41695 | 91.47 | 2.69 |
| zomato | pickup | 45584 | 41513 | 91.07 | 1.18 |

## Data-quality checks (failing rows caught)
| layer | check | dataset | rows checked | rows failed |
|---|---|---|---|---|
| batch | coords_outside_india | amazon | 43739 | 3693 |
| batch | coords_outside_india | zomato | 45584 | 4071 |
| batch | duplicate_order_id | amazon | 43739 | 0 |
| batch | duplicate_order_id | zomato | 45584 | 0 |
| batch | fct_rows_marked_invalid | fct_deliveries | 89323 | 12852 |
| batch | negative_or_absurd_delivery_time | amazon | 43739 | 0 |
| batch | negative_or_absurd_delivery_time | zomato | 45584 | 0 |
| batch | null_order_id | amazon | 43739 | 0 |
| batch | null_order_id | zomato | 45584 | 0 |
| batch | null_required_field | amazon | 43739 | 0 |
| batch | null_required_field | zomato | 45584 | 0 |
| batch | orphan_pincode | amazon | 43739 | 0 |
| batch | orphan_pincode | zomato | 45584 | 0 |
| batch | pincode_unmatched | amazon | 43739 | 3926 |
| batch | pincode_unmatched | zomato | 45584 | 4312 |
| batch | unparseable_timestamp | amazon | 43739 | 91 |
| batch | unparseable_timestamp | zomato | 45584 | 5799 |
| stream | duplicate_event | stream | 1000000 | 19577 |
| stream | late_arriving_ping | gps.pings | 960148 | 9760 |
| stream | malformed_event | stream | 1000000 | 1983 |
| stream | out_of_bounds_gps | stream | 978440 | 4897 |

## Streaming (SIMULATED events)
| load_id | produced | consumed | landed | duplicates dropped | late | out of bounds | malformed | seconds | events/s |
|---|---|---|---|---|---|---|---|---|---|
| 20261010T124209 | 1000000 | 1000000 | 973543 | 19577 | 9760 | 4897 | 1983 | 134.8 | 7418 |

## Delay impact (Zomato)
| factor | value | deliveries | avg min | delay vs baseline min |
|---|---|---|---|---|
| festival | true | 708 | 45.5 | 19.5 |
| festival | false | 35583 | 26.0 | 0.0 |
| rain_bucket | heavy | 58 | 28.2 | 2.3 |
| rain_bucket | none | 31898 | 26.3 | 0.4 |
| rain_bucket | light | 3818 | 26.2 | 0.3 |
| rain_bucket | moderate | 603 | 25.9 | 0.0 |
| traffic | jam | 11522 | 31.2 | 9.9 |
| traffic | high | 3567 | 27.2 | 5.9 |
| traffic | medium | 8944 | 26.7 | 5.4 |
| traffic | low | 12441 | 21.3 | 0.0 |
| weather | cloudy | 6067 | 29.0 | 7.0 |
| weather | fog | 6190 | 29.0 | 7.0 |
| weather | windy | 6062 | 26.2 | 4.2 |
| weather | stormy | 6158 | 25.9 | 3.9 |
| weather | sandstorms | 6054 | 25.9 | 3.9 |
| weather | sunny | 5943 | 22.0 | 0.0 |

## City comparison (top 12 by volume)
| source | city | deliveries | avg min | % over 45 min |
|---|---|---|---|---|
| amazon | Jaipur | 3307 | 127.1 | 94.3 |
| amazon | Coimbatore | 3050 | 125.5 | 94.4 |
| amazon | Bengaluru | 3047 | 124.9 | 94.7 |
| amazon | Surat | 3046 | 123.6 | 94.0 |
| amazon | Hyderabad | 3045 | 124.8 | 93.3 |
| zomato | Jaipur | 3037 | 26.7 | 3.6 |
| amazon | Mumbai | 3037 | 124.0 | 94.4 |
| amazon | Indore | 3029 | 123.9 | 93.7 |
| amazon | Pune | 2988 | 124.7 | 94.0 |
| amazon | Mysuru | 2877 | 122.5 | 93.0 |
| amazon | Chennai | 2850 | 125.3 | 93.9 |
| zomato | Bengaluru | 2782 | 26.1 | 2.8 |

## Busiest pincode-hours
| source | pincode | city | hour | deliveries |
|---|---|---|---|---|
| amazon | 453771 | Indore | 23 | 247 |
| amazon | 453771 | Indore | 19 | 240 |
| amazon | 453771 | Indore | 21 | 238 |
| zomato | 453771 | Indore | 17 | 222 |
| amazon | 453771 | Indore | 18 | 221 |
| amazon | 453771 | Indore | 20 | 221 |
| amazon | 453771 | Indore | 17 | 220 |
| zomato | 453771 | Indore | 21 | 218 |
| zomato | 453771 | Indore | 19 | 216 |
| zomato | 453771 | Indore | 23 | 216 |

## Courier utilisation (distribution)
| courier-city rows | mean | median | max |
|---|---|---|---|
| 1170 | 0.346 | 0.307 | 1.000 |

## Warehouse row counts
| table | rows |
|---|---|
| analytics.dim_city | 26 |
| analytics.dim_courier | 1320 |
| analytics.dim_date | 117 |
| analytics.dim_pincode_zone | 19586 |
| analytics.dim_time | 24 |
| analytics.dim_weather_daily | 1320 |
| analytics.fct_deliveries | 89323 |
| spark_marts.city_comparison | 52 |
| spark_marts.courier_utilisation | 1170 |
| spark_marts.delay_impact | 30 |
| spark_marts.deliveries_per_pincode_hour | 7154 |

## Runtime (seconds per step)
| step | seconds |
|---|---|
| services_up | 15.2 |
| acquire | 0.1 |
| load_raw | 3.5 |
| geo_enrich | 11.8 |
| weather | 0.3 |
| dbt_build | 13.0 |
| quality | 0.2 |
| spark | 31.9 |
| streaming | 268.2 |

**Total wall clock: 344 s**
