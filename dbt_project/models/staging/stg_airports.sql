select
    airport_code,
    airport_name,
    city,
    country,
    latitude,
    longitude
from {{ source('raw', 'airports') }}

