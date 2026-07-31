select
    flight_id,
    flight_date,
    flight_number,
    origin_airport_code,
    destination_airport_code,
    scheduled_departure_ts,
    actual_departure_ts,
    scheduled_arrival_ts,
    actual_arrival_ts,
    status,
    aircraft_type,
    passenger_count,
    dateDiff('minute', scheduled_departure_ts, actual_departure_ts) as departure_delay_minutes,
    dateDiff('minute', scheduled_arrival_ts, actual_arrival_ts) as arrival_delay_minutes
from {{ source('raw', 'flights') }}
