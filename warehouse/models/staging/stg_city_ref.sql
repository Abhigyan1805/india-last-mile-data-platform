select city, state, latitude, longitude from {{ source('raw', 'city_ref') }}
