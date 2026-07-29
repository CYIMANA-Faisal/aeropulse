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
    departure_delay_minutes,
    arrival_delay_minutes,
    case
        when arrival_delay_minutes is null then 'not_arrived'
        when arrival_delay_minutes <= 0 then 'early_or_on_time'
        when arrival_delay_minutes <= 15 then 'minor_delay'
        else 'major_delay'
    end as arrival_performance
from {{ ref('stg_flights') }}

