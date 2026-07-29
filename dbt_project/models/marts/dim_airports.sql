select
    airport_code,
    airport_name,
    city,
    country,
    latitude,
    longitude
from {{ ref('stg_airports') }}

