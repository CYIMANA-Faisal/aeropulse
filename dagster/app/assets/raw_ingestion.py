import csv
import json
import os
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

import clickhouse_connect
from dagster import AssetKey, asset, get_dagster_logger

RAW_DATABASE = os.getenv("CLICKHOUSE_RAW_DATABASE", "aeropulse_raw")
DEFAULT_SOURCES_DIR = Path("/opt/sources")
REPO_SOURCES_DIR = Path(__file__).resolve().parents[3] / "sources"
DEFAULT_AIRPORTS_SOURCE_FILE = "airports.csv"
DEFAULT_FLIGHTS_SOURCE_FILE = "flight_data_2024.csv"
DEFAULT_DELAY_CAUSES_SOURCE_FILE = "Airline_Delay_Cause.csv"

AIRPORT_COLUMNS = [
    "airport_code",
    "airport_name",
    "city",
    "country",
    "latitude",
    "longitude",
]

FLIGHT_COLUMNS = [
    "flight_id",
    "flight_date",
    "flight_number",
    "origin_airport_code",
    "destination_airport_code",
    "scheduled_departure_ts",
    "actual_departure_ts",
    "scheduled_arrival_ts",
    "actual_arrival_ts",
    "status",
    "aircraft_type",
    "passenger_count",
]

DELAY_CAUSE_COLUMNS = [
    "year",
    "month",
    "carrier",
    "carrier_name",
    "airport",
    "airport_name",
    "arr_flights",
    "arr_del15",
    "carrier_ct",
    "weather_ct",
    "nas_ct",
    "security_ct",
    "late_aircraft_ct",
    "arr_cancelled",
    "arr_diverted",
    "arr_delay",
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay",
]


def _client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "aeropulse"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "aeropulse"),
    )


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value in (None, ""):
        return default

    return int(value)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default

    return value.lower() in {"1", "true", "t", "yes", "y", "on"}


def _source_file(prefix: str, default_filename: str) -> Path | None:
    configured_file = os.getenv(f"{prefix}_SOURCE_FILE")
    configured_dir = os.getenv("SOURCES_DIR")
    candidates = []

    if configured_file:
        candidates.append(Path(configured_file))
    if configured_dir:
        candidates.append(Path(configured_dir) / default_filename)

    candidates.extend(
        [
            DEFAULT_SOURCES_DIR / default_filename,
            REPO_SOURCES_DIR / default_filename,
        ]
    )

    for path in candidates:
        if path.exists():
            return path

    return None


def _csv_records(path: Path):
    with path.open(newline="", encoding="utf-8-sig", errors="replace") as handle:
        yield from csv.DictReader(handle)


def _nested_value(data, path: str):
    current = data
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]

    return current


def _pick(data: dict, *paths: str):
    for path in paths:
        value = _nested_value(data, path)
        if value not in (None, ""):
            return value

    return None


def _extract_records(payload, data_path: str | None):
    if data_path:
        payload = _nested_value(payload, data_path)

    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        for key in ("data", "results", "items", "records"):
            value = payload.get(key)
            if isinstance(value, list):
                return value

    raise ValueError("API response must be a JSON array or contain data/results/items/records")


def _api_headers(prefix: str) -> dict[str, str]:
    token = os.getenv(f"{prefix}_API_KEY") or os.getenv("API_KEY")
    if not token:
        return {}

    header_name = os.getenv(f"{prefix}_API_HEADER_NAME") or os.getenv("API_HEADER_NAME", "Authorization")
    auth_scheme = os.getenv(f"{prefix}_API_AUTH_SCHEME") or os.getenv("API_AUTH_SCHEME", "Bearer")
    header_value = token if not auth_scheme else f"{auth_scheme} {token}"

    return {header_name: header_value}


def _fetch_json(prefix: str):
    url = os.getenv(f"{prefix}_API_URL")
    if not url:
        return None

    timeout = int(os.getenv("API_TIMEOUT_SECONDS", "20"))
    request = Request(url, headers=_api_headers(prefix))

    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except (OSError, URLError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Failed to fetch {prefix.lower()} API data from {url}: {exc}") from exc


def _to_float(value, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    return float(value)


def _optional_float(value) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _to_int(value, default: int = 0) -> int:
    if value in (None, ""):
        return default
    return int(value)


def _parse_date(value, fallback: date | None = None) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str) and value:
        return datetime.fromisoformat(value[:10]).date()
    if fallback:
        return fallback

    return date.today()


def _parse_datetime(value) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))

    if parsed.tzinfo:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)

    return parsed


def _clean_integral(value) -> str | None:
    if value in (None, ""):
        return None

    text = str(value).strip()
    try:
        number = float(text)
    except ValueError:
        return text

    if number.is_integer():
        return str(int(number))

    return text


def _truthy_number(value) -> bool:
    return bool(_optional_float(value) or 0)


def _city_from_us_label(value) -> str:
    if not value:
        return "Unknown"

    label = str(value).split(":", 1)[0].strip()
    city = label.split(",", 1)[0].strip()
    return city or "Unknown"


def _normalize_delay_airport(record: dict):
    airport_code = _pick(record, "airport", "airport_code", "iata_code", "iata", "code")
    if not airport_code:
        return None

    airport_name = _pick(record, "airport_name", "name") or str(airport_code).upper()
    city = _city_from_us_label(airport_name)

    return (str(airport_code).upper(), airport_name, city, "United States", 0.0, 0.0)


def _normalize_bts_airport(record: dict, prefix: str):
    airport_code = _pick(record, prefix)
    if not airport_code:
        return None

    city_name = _pick(record, f"{prefix}_city_name")
    state_name = _pick(record, f"{prefix}_state_nm")
    city = _city_from_us_label(city_name)
    airport_name = city_name or f"{str(airport_code).upper()} Airport"
    country = "United States" if city_name or state_name else "Unknown"

    return (str(airport_code).upper(), airport_name, city, country, 0.0, 0.0)


def _parse_bts_time(value) -> tuple[time, int] | None:
    text = _clean_integral(value)
    if not text:
        return None

    digits = "".join(character for character in text if character.isdigit())
    if not digits:
        return None

    encoded_time = int(digits)
    hour = encoded_time // 100
    minute = encoded_time % 100
    day_offset = 0

    if hour == 24:
        hour = 0
        day_offset = 1
    if hour > 24 or minute > 59:
        return None

    return time(hour=hour, minute=minute), day_offset


def _combine_bts_datetime(flight_date: date, value) -> datetime | None:
    parsed_time = _parse_bts_time(value)
    if not parsed_time:
        return None

    parsed, day_offset = parsed_time
    return datetime.combine(flight_date + timedelta(days=day_offset), parsed)


def _scheduled_arrival_datetime(flight_date: date, scheduled_departure: datetime | None, arrival_time) -> datetime | None:
    scheduled_arrival = _combine_bts_datetime(flight_date, arrival_time)
    if scheduled_departure and scheduled_arrival and scheduled_arrival <= scheduled_departure:
        scheduled_arrival += timedelta(days=1)

    return scheduled_arrival


def _actual_datetime(scheduled: datetime | None, clock_value, delay_minutes) -> datetime | None:
    if clock_value in (None, ""):
        return None

    delay = _optional_float(delay_minutes)
    if scheduled and delay is not None:
        return scheduled + timedelta(minutes=delay)

    if scheduled:
        return _combine_bts_datetime(scheduled.date(), clock_value)

    return None


def _clean_flight_number(carrier: str | None, number) -> str | None:
    clean_number = _clean_integral(number)
    if not clean_number:
        return None

    return f"{carrier}{clean_number}" if carrier else clean_number


def _normalize_bts_flight(record: dict, index: int):
    flight_date = _parse_date(_pick(record, "fl_date", "flight_date", "date"))
    origin = _pick(record, "origin")
    destination = _pick(record, "dest")
    carrier = _pick(record, "op_unique_carrier", "carrier")
    flight_number = _clean_flight_number(carrier, _pick(record, "op_carrier_fl_num", "flight_number"))

    if not flight_number or not origin or not destination:
        return None

    scheduled_departure = _combine_bts_datetime(flight_date, _pick(record, "crs_dep_time"))
    scheduled_arrival = _scheduled_arrival_datetime(flight_date, scheduled_departure, _pick(record, "crs_arr_time"))

    elapsed_minutes = _optional_float(_pick(record, "crs_elapsed_time"))
    if scheduled_departure and not scheduled_arrival:
        scheduled_arrival = scheduled_departure + timedelta(minutes=elapsed_minutes or 60)
    if not scheduled_departure or not scheduled_arrival:
        return None

    cancelled = _truthy_number(_pick(record, "cancelled"))
    diverted = _truthy_number(_pick(record, "diverted"))
    actual_departure = None if cancelled else _actual_datetime(
        scheduled_departure,
        _pick(record, "dep_time"),
        _pick(record, "dep_delay"),
    )
    actual_arrival = None if cancelled or diverted else _actual_datetime(
        scheduled_arrival,
        _pick(record, "arr_time"),
        _pick(record, "arr_delay"),
    )

    arrival_delay = _optional_float(_pick(record, "arr_delay"))
    if cancelled:
        status = "cancelled"
    elif diverted:
        status = "diverted"
    elif arrival_delay is not None and arrival_delay > 15:
        status = "delayed"
    elif actual_arrival:
        status = "arrived"
    else:
        status = "scheduled"

    flight_id = f"{flight_date.isoformat()}-{carrier or 'NA'}-{flight_number}-{origin}-{destination}-{index}"

    return (
        flight_id,
        flight_date,
        flight_number,
        str(origin).upper(),
        str(destination).upper(),
        scheduled_departure,
        actual_departure,
        scheduled_arrival,
        actual_arrival,
        status,
        None,
        0,
    )


def _normalize_delay_cause(record: dict):
    return (
        _to_int(_pick(record, "year"), default=0),
        _to_int(_pick(record, "month"), default=0),
        _pick(record, "carrier") or "",
        _pick(record, "carrier_name") or "",
        str(_pick(record, "airport") or "").upper(),
        _pick(record, "airport_name") or "",
        _to_float(_pick(record, "arr_flights")),
        _to_float(_pick(record, "arr_del15")),
        _to_float(_pick(record, "carrier_ct")),
        _to_float(_pick(record, "weather_ct")),
        _to_float(_pick(record, "nas_ct")),
        _to_float(_pick(record, "security_ct")),
        _to_float(_pick(record, "late_aircraft_ct")),
        _to_float(_pick(record, "arr_cancelled")),
        _to_float(_pick(record, "arr_diverted")),
        _to_float(_pick(record, "arr_delay")),
        _to_float(_pick(record, "carrier_delay")),
        _to_float(_pick(record, "weather_delay")),
        _to_float(_pick(record, "nas_delay")),
        _to_float(_pick(record, "security_delay")),
        _to_float(_pick(record, "late_aircraft_delay")),
    )


def _sample_airport_rows():
    return [
        ("JFK", "John F. Kennedy International Airport", "New York", "United States", 40.6413, -73.7781),
        ("LAX", "Los Angeles International Airport", "Los Angeles", "United States", 33.9416, -118.4085),
        ("ORD", "O'Hare International Airport", "Chicago", "United States", 41.9742, -87.9073),
        ("ATL", "Hartsfield-Jackson Atlanta International Airport", "Atlanta", "United States", 33.6407, -84.4277),
    ]


def _sample_flight_rows():
    return [
        (
            "FL-1001",
            date(2026, 7, 20),
            "AP1001",
            "JFK",
            "LAX",
            datetime(2026, 7, 20, 8, 0, 0),
            datetime(2026, 7, 20, 8, 12, 0),
            datetime(2026, 7, 20, 11, 15, 0),
            datetime(2026, 7, 20, 11, 32, 0),
            "arrived",
            "A321",
            178,
        ),
        (
            "FL-1002",
            date(2026, 7, 20),
            "AP1002",
            "LAX",
            "ORD",
            datetime(2026, 7, 20, 13, 30, 0),
            datetime(2026, 7, 20, 13, 26, 0),
            datetime(2026, 7, 20, 19, 40, 0),
            datetime(2026, 7, 20, 19, 35, 0),
            "arrived",
            "B737",
            151,
        ),
        (
            "FL-1003",
            date(2026, 7, 21),
            "AP1003",
            "ATL",
            "JFK",
            datetime(2026, 7, 21, 6, 45, 0),
            datetime(2026, 7, 21, 7, 5, 0),
            datetime(2026, 7, 21, 9, 0, 0),
            datetime(2026, 7, 21, 9, 22, 0),
            "arrived",
            "A220",
            112,
        ),
        (
            "FL-1004",
            date(2026, 7, 21),
            "AP1004",
            "ORD",
            "ATL",
            datetime(2026, 7, 21, 15, 10, 0),
            None,
            datetime(2026, 7, 21, 18, 5, 0),
            None,
            "scheduled",
            "B737",
            0,
        ),
    ]


def _normalize_airport(record: dict):
    airport_code = _pick(record, "airport_code", "iata_code", "iata", "iataCode", "code")
    if not airport_code:
        return None

    airport_name = _pick(record, "airport_name", "name", "airport.name") or airport_code
    city = _pick(record, "city", "city_name", "municipality", "location.city") or "Unknown"
    country_value = _pick(record, "country", "country_name", "iso_country", "location.country")
    country = "United States" if country_value == "US" else country_value or "Unknown"
    latitude = _to_float(_pick(record, "latitude", "latitude_deg", "lat", "location.latitude", "geo.lat"))
    longitude = _to_float(
        _pick(record, "longitude", "longitude_deg", "lon", "lng", "location.longitude", "geo.lon", "geo.lng")
    )

    return (str(airport_code).upper(), airport_name, city, country, latitude, longitude)


def _normalize_flight(record: dict, index: int):
    flight_number = _pick(record, "flight_number", "number", "flight.iata", "flight.icao", "callsign")
    origin = _pick(
        record,
        "origin_airport_code",
        "origin",
        "departure_iata",
        "departure.iata",
        "departure.airport_code",
        "departure.airport.iata",
    )
    destination = _pick(
        record,
        "destination_airport_code",
        "destination",
        "arrival_iata",
        "arrival.iata",
        "arrival.airport_code",
        "arrival.airport.iata",
    )

    if not flight_number or not origin or not destination:
        return None

    scheduled_departure = _parse_datetime(
        _pick(record, "scheduled_departure_ts", "scheduled_departure", "departure.scheduled", "departure.scheduled_time")
    )
    actual_departure = _parse_datetime(
        _pick(record, "actual_departure_ts", "actual_departure", "departure.actual", "departure.actual_time")
    )
    scheduled_arrival = _parse_datetime(
        _pick(record, "scheduled_arrival_ts", "scheduled_arrival", "arrival.scheduled", "arrival.scheduled_time")
    )
    actual_arrival = _parse_datetime(
        _pick(record, "actual_arrival_ts", "actual_arrival", "arrival.actual", "arrival.actual_time")
    )

    flight_date = _parse_date(
        _pick(record, "flight_date", "date", "departure.scheduled", "scheduled_departure_ts"),
        fallback=scheduled_departure.date() if scheduled_departure else None,
    )

    if not scheduled_departure:
        scheduled_departure = datetime.combine(flight_date, time.min)
    if not scheduled_arrival:
        scheduled_arrival = scheduled_departure + timedelta(hours=1)

    flight_id = _pick(record, "flight_id", "id", "flight.id") or (
        f"{flight_number}-{flight_date.isoformat()}-{origin}-{destination}-{index}"
    )
    status = _pick(record, "status", "flight_status") or "unknown"
    aircraft_type = _pick(record, "aircraft_type", "aircraft.iata", "aircraft.icao", "aircraft.registration")
    passenger_count = _to_int(_pick(record, "passenger_count", "passengers"), default=0)

    return (
        str(flight_id),
        flight_date,
        str(flight_number),
        str(origin).upper(),
        str(destination).upper(),
        scheduled_departure,
        actual_departure,
        scheduled_arrival,
        actual_arrival,
        str(status),
        str(aircraft_type) if aircraft_type else None,
        passenger_count,
    )


def _dedupe(rows, key_index: int = 0):
    seen = set()
    result = []
    for row in rows:
        key = row[key_index]
        if key in seen:
            continue
        seen.add(key)
        result.append(row)

    return result


def _rows_from_api(prefix: str, normalizer, sample_rows):
    payload = _fetch_json(prefix)
    if payload is None:
        return sample_rows()

    try:
        data_path = os.getenv(f"{prefix}_API_DATA_PATH")
        limit = int(os.getenv(f"{prefix}_API_LIMIT", "500"))
        records = _extract_records(payload, data_path)
        rows = [normalizer(record, index) for index, record in enumerate(records, start=1)]
        rows = _dedupe([row for row in rows if row])[:limit]
        if rows:
            return rows
        raise ValueError("API response did not contain any usable records")
    except Exception as exc:
        if not _env_bool("API_FALLBACK_TO_SAMPLE", True):
            raise

        get_dagster_logger().warning("%s API data was unusable; using sample data instead: %s", prefix.lower(), exc)
        return sample_rows()


def _airport_rows_from_source() -> list[tuple] | None:
    source_path = _source_file("AIRPORTS", DEFAULT_AIRPORTS_SOURCE_FILE)
    if not source_path:
        source_path = _source_file("DELAY_CAUSES", DEFAULT_DELAY_CAUSES_SOURCE_FILE)
    if not source_path:
        return None

    rows_by_code = {}
    for record in _csv_records(source_path):
        row = _normalize_airport(record) or _normalize_delay_airport(record)
        if row:
            rows_by_code.setdefault(row[0], row)

    rows = list(rows_by_code.values())
    if rows:
        get_dagster_logger().info("Loaded %s airports from %s", len(rows), source_path)
        return rows

    return None


def _airport_rows_from_api():
    return _rows_from_api("AIRPORTS", lambda record, _: _normalize_airport(record), _sample_airport_rows)


def _flight_rows_from_api():
    return _rows_from_api("FLIGHTS", _normalize_flight, _sample_flight_rows)


def _insert_rows(client, table_name: str, rows, column_names: list[str]) -> int:
    batch_size = _env_int("SOURCE_BATCH_SIZE", 50_000)
    batch = []
    inserted = 0

    for row in rows:
        if not row:
            continue

        batch.append(row)
        if len(batch) >= batch_size:
            client.insert(table_name, batch, column_names=column_names)
            inserted += len(batch)
            batch.clear()

    if batch:
        client.insert(table_name, batch, column_names=column_names)
        inserted += len(batch)

    return inserted


def _ensure_raw_database(client) -> None:
    client.command(f"create database if not exists {RAW_DATABASE}")


def _ensure_airports_table(client) -> None:
    client.command(
        f"""
        create table if not exists {RAW_DATABASE}.airports (
            airport_code String,
            airport_name String,
            city String,
            country String,
            latitude Float64,
            longitude Float64
        )
        engine = MergeTree
        order by airport_code
        """
    )


def _ensure_flights_table(client) -> None:
    client.command(
        f"""
        create table if not exists {RAW_DATABASE}.flights (
            flight_id String,
            flight_date Date,
            flight_number String,
            origin_airport_code String,
            destination_airport_code String,
            scheduled_departure_ts DateTime,
            actual_departure_ts Nullable(DateTime),
            scheduled_arrival_ts DateTime,
            actual_arrival_ts Nullable(DateTime),
            status LowCardinality(String),
            aircraft_type Nullable(String),
            passenger_count UInt16
        )
        engine = MergeTree
        order by (flight_date, flight_id)
        """
    )


def _ensure_delay_causes_table(client) -> None:
    client.command(
        f"""
        create table if not exists {RAW_DATABASE}.airline_delay_causes (
            year UInt16,
            month UInt8,
            carrier String,
            carrier_name String,
            airport String,
            airport_name String,
            arr_flights Float64,
            arr_del15 Float64,
            carrier_ct Float64,
            weather_ct Float64,
            nas_ct Float64,
            security_ct Float64,
            late_aircraft_ct Float64,
            arr_cancelled Float64,
            arr_diverted Float64,
            arr_delay Float64,
            carrier_delay Float64,
            weather_delay Float64,
            nas_delay Float64,
            security_delay Float64,
            late_aircraft_delay Float64
        )
        engine = MergeTree
        order by (year, month, carrier, airport)
        """
    )


def _append_missing_airports(client, airport_rows_by_code: dict[str, tuple]) -> None:
    if not airport_rows_by_code:
        return

    existing = {
        row[0]
        for row in client.query(f"select airport_code from {RAW_DATABASE}.airports").result_rows
    }
    missing_rows = [row for code, row in airport_rows_by_code.items() if code not in existing]
    if missing_rows:
        client.insert(f"{RAW_DATABASE}.airports", missing_rows, column_names=AIRPORT_COLUMNS)


def _load_flights_from_source(client, path: Path) -> int:
    limit = _env_int("FLIGHTS_SOURCE_LIMIT", 0)
    airport_rows_by_code = {}

    def rows():
        for index, record in enumerate(_csv_records(path), start=1):
            if limit and index > limit:
                break

            for prefix in ("origin", "dest"):
                airport_row = _normalize_bts_airport(record, prefix)
                if airport_row:
                    airport_rows_by_code.setdefault(airport_row[0], airport_row)

            yield _normalize_bts_flight(record, index)

    inserted = _insert_rows(client, f"{RAW_DATABASE}.flights", rows(), FLIGHT_COLUMNS)
    _append_missing_airports(client, airport_rows_by_code)
    get_dagster_logger().info("Loaded %s flights from %s", inserted, path)

    return inserted


def _delay_cause_rows_from_source(path: Path):
    limit = _env_int("DELAY_CAUSES_SOURCE_LIMIT", 0)

    for index, record in enumerate(_csv_records(path), start=1):
        if limit and index > limit:
            break

        yield _normalize_delay_cause(record)


@asset(group_name="raw")
def raw_airports() -> int:
    rows = _airport_rows_from_source() or _airport_rows_from_api()

    client = _client()
    _ensure_raw_database(client)
    _ensure_airports_table(client)
    client.command(f"truncate table {RAW_DATABASE}.airports")
    _insert_rows(client, f"{RAW_DATABASE}.airports", rows, AIRPORT_COLUMNS)

    return len(rows)


@asset(deps=[AssetKey("raw_airports")], group_name="raw")
def raw_flights() -> int:
    client = _client()
    _ensure_raw_database(client)
    _ensure_airports_table(client)
    _ensure_flights_table(client)
    client.command(f"truncate table {RAW_DATABASE}.flights")

    source_path = _source_file("FLIGHTS", DEFAULT_FLIGHTS_SOURCE_FILE)
    if source_path:
        return _load_flights_from_source(client, source_path)

    rows = _flight_rows_from_api()
    return _insert_rows(client, f"{RAW_DATABASE}.flights", rows, FLIGHT_COLUMNS)


@asset(group_name="raw")
def raw_delay_causes() -> int:
    client = _client()
    _ensure_raw_database(client)
    _ensure_delay_causes_table(client)
    client.command(f"truncate table {RAW_DATABASE}.airline_delay_causes")

    source_path = _source_file("DELAY_CAUSES", DEFAULT_DELAY_CAUSES_SOURCE_FILE)
    if not source_path:
        get_dagster_logger().warning("Delay-cause source file was not found; loaded 0 rows")
        return 0

    inserted = _insert_rows(
        client,
        f"{RAW_DATABASE}.airline_delay_causes",
        _delay_cause_rows_from_source(source_path),
        DELAY_CAUSE_COLUMNS,
    )
    get_dagster_logger().info("Loaded %s delay-cause rows from %s", inserted, source_path)

    return inserted
