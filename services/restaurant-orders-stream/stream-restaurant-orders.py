import csv
import io
import json
import os
import time
import zipfile
from contextlib import contextmanager
from datetime import datetime, time as datetime_time
from pathlib import Path

from confluent_kafka import KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext


DATE_FORMAT = "%d/%m/%Y %H:%M"
WEATHER_DATE_FORMAT = "%Y%m%d"
WEATHER_FILENAME = "london_weather.csv"
WEATHER_TOPIC = "weather-london"
WEATHER_PREDICTIONS_TOPIC = "weather-predictions-london"
FORECAST_DAYS = 7
DATA_DIRECTORY = Path(__file__).resolve().parent
INPUT_ARCHIVE = "streaming-inputs.zip"
INPUTS = (
    ("restaurant-1", "restaurant-1-orders.csv"),
    ("restaurant-2", "restaurant-2-orders.csv"),
)
ORDER_SCHEMA = json.dumps(
    {
        "type": "record",
        "name": "RestaurantOrder",
        "fields": [
            {"name": "source", "type": "string"},
            {
                "name": "record",
                "type": {
                    "type": "record",
                    "name": "RestaurantOrderRecord",
                    "fields": [
                        {"name": "order_id", "type": "int"},
                        {"name": "order_date", "type": "string"},
                        {"name": "item_name", "type": "string"},
                        {"name": "quantity", "type": "int"},
                        {"name": "product_price", "type": "double"},
                        {"name": "total_products", "type": "int"},
                    ],
                },
            },
        ],
    }
)

WEATHER_SCHEMA = json.dumps(
    {
        "type": "record",
        "name": "Weather",
        "fields": [
            {"name": "date", "type": "string"},
            {"name": "cloud_cover", "type": ["null", "double"]},
            {"name": "sunshine", "type": ["null", "double"]},
            {"name": "global_radiation", "type": ["null", "double"]},
            {"name": "max_temp", "type": ["null", "double"]},
            {"name": "mean_temp", "type": ["null", "double"]},
            {"name": "min_temp", "type": ["null", "double"]},
            {"name": "precipitation", "type": ["null", "double"]},
            {"name": "pressure", "type": ["null", "double"]},
            {"name": "snow_depth", "type": ["null", "double"]},
        ],
    }
)

FORECAST_SCHEMA = json.dumps(
    {
        "type": "record",
        "name": "WeatherForecast",
        "fields": [
            {"name": "issued_at", "type": "string"},
            {
                "name": "forecasts",
                "type": {
                    "type": "array",
                    "items": json.loads(WEATHER_SCHEMA),
                },
            },
        ],
    }
)

def send_record(producer, topic, key, value, serializer):
    delivered = []
    producer.produce(
        topic,
        key=key,
        value=serializer(value, SerializationContext(topic, MessageField.VALUE)),
        on_delivery=lambda error, message: delivered.append(error),
    )
    deadline = time.monotonic() + 30
    while not delivered:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Timed out sending a record to Kafka topic {topic}")
        producer.poll(min(1, remaining))
    if delivered[0] is not None:
        raise KafkaException(delivered[0])


def restaurant_order_subject_name_strategy(_context, _record_name):
    return "RestaurantOrder"


def weather_subject_name_strategy(_context, _record_name):
    return "Weather"


def weather_forecast_subject_name_strategy(_context, _record_name):
    return "WeatherForecast"


@contextmanager
def open_input(filename):
    archive_path = DATA_DIRECTORY / INPUT_ARCHIVE
    if archive_path.is_file():
        with zipfile.ZipFile(archive_path) as archive:
            with archive.open(filename) as binary_file:
                with io.TextIOWrapper(
                    binary_file,
                    encoding="utf-8-sig",
                    newline="",
                ) as csv_file:
                    yield csv_file
        return

    with (DATA_DIRECTORY / filename).open(
        newline="",
        encoding="utf-8-sig",
    ) as csv_file:
        yield csv_file


def load_records():
    records = []
    for source, filename in INPUTS:
        path = DATA_DIRECTORY / filename
        with open_input(filename) as csv_file:
            reader = csv.DictReader(csv_file)
            if not reader.fieldnames or "Order Date" not in reader.fieldnames:
                raise ValueError(f"{path} is missing the 'Order Date' column")

            for line_number, row in enumerate(reader, start=2):
                try:
                    event_time = datetime.strptime(row["Order Date"], DATE_FORMAT)
                except (TypeError, ValueError) as error:
                    raise ValueError(
                        f"Invalid Order Date in {path} at line {line_number}: "
                        f"{row.get('Order Date')!r}"
                    ) from error
                records.append((event_time, source, row))

    records.sort(key=lambda item: (item[0], item[1]))
    if not records:
        raise ValueError("The streaming CSV files contain no records")
    return records


def load_weather_records():
    path = DATA_DIRECTORY / WEATHER_FILENAME
    records = []
    with open_input(WEATHER_FILENAME) as csv_file:
        reader = csv.DictReader(csv_file)
        if not reader.fieldnames or "date" not in reader.fieldnames:
            raise ValueError(f"{path} is missing the 'date' column")

        for line_number, row in enumerate(reader, start=2):
            try:
                event_date = datetime.strptime(row["date"], WEATHER_DATE_FORMAT)
            except (TypeError, ValueError) as error:
                raise ValueError(
                    f"Invalid date in {path} at line {line_number}: "
                    f"{row.get('date')!r}"
                ) from error
            records.append((event_date, row))

    records.sort(key=lambda item: item[0])
    if not records:
        raise ValueError(f"The weather CSV contains no records: {path}")
    return records


def order_key(source, row):
    key_field = "Order Number" if source == "restaurant-1" else "Order ID"
    return row.get(key_field)


def normalize_order_record(source, row):
    return {
        "order_id": int(
            row["Order Number"] if source == "restaurant-1" else row["Order ID"]
        ),
        "order_date": row["Order Date"],
        "item_name": row["Item Name"],
        "quantity": int(row["Quantity"]),
        "product_price": float(row["Product Price"]),
        "total_products": int(row["Total products"]),
    }


def optional_float(value):
    if value is None or not str(value).strip():
        return None
    return float(value)


def normalize_weather_record(row):
    return {
        "date": row["date"],
        "cloud_cover": optional_float(row.get("cloud_cover")),
        "sunshine": optional_float(row.get("sunshine")),
        "global_radiation": optional_float(row.get("global_radiation")),
        "max_temp": optional_float(row.get("max_temp")),
        "mean_temp": optional_float(row.get("mean_temp")),
        "min_temp": optional_float(row.get("min_temp")),
        "precipitation": optional_float(row.get("precipitation")),
        "pressure": optional_float(row.get("pressure")),
        "snow_depth": optional_float(row.get("snow_depth")),
    }


def main():
    records = load_records()
    weather_records = load_weather_records()
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    schema_registry_url = os.environ.get(
        "KAFKA_SCHEMA_REGISTRY_URL", "http://kafka-schema-registry:8081"
    )
    topic = os.environ.get("KAFKA_TOPIC", "restaurant-orders")
    schema_registry = SchemaRegistryClient({"url": schema_registry_url})
    order_serializer = AvroSerializer(
        schema_registry,
        ORDER_SCHEMA,
        conf={
            "subject.name.strategy": restaurant_order_subject_name_strategy,
        },
    )

    weather_serializer = AvroSerializer(
        schema_registry,
        WEATHER_SCHEMA,
        conf={"subject.name.strategy": weather_subject_name_strategy},
    )

    forecast_serializer = AvroSerializer(
        schema_registry,
        FORECAST_SCHEMA,
        conf={"subject.name.strategy": weather_forecast_subject_name_strategy},
    )

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "retries": 5,
        }
    )
    events = [
        (event_time, "order", source, -1, row)
        for event_time, source, row in records
    ]
    events.extend(
        (
            datetime.combine(event_date.date(), datetime.min.time()),
            "forecast",
            "",
            index,
            row,
        )
        for index, (event_date, row) in enumerate(weather_records)
        if index + FORECAST_DAYS <= len(weather_records)
    )
    events.extend(
        (
            datetime.combine(event_date.date(), datetime_time(23, 59)),
            "weather",
            "",
            index,
            row,
        )
        for index, (event_date, row) in enumerate(weather_records)
    )
    # Sort event by datetime, then by event type to ensure that forecasts are sent before the weather observations for the same day
    events.sort(key=lambda item: (item[0], item[1]))
    
    simulation_start = min(records[0][0], weather_records[0][0])
    started_at = time.monotonic()
    print(
        f"Streaming {len(records)} order records to {topic} and "
        f"{len(weather_records)} weather records to {WEATHER_TOPIC}; "
        f"event range {simulation_start} through {events[-1][0]} "
        "(1 event-day = 60 seconds)",
        flush=True,
    )

    try:
        order_count = 0
        for event_time, event_type, source, weather_index, row in events:
            elapsed_event_days = (
                event_time - simulation_start
            ).total_seconds() / 86400
            target_elapsed = elapsed_event_days * 60
            wait_seconds = target_elapsed - (time.monotonic() - started_at)
            if wait_seconds > 0:
                time.sleep(wait_seconds)

            if event_type == "order":
                send_record(
                    producer,
                    topic,
                    key=order_key(source, row),
                    value={
                        "source": source,
                        "record": normalize_order_record(source, row),
                    },
                    serializer=order_serializer,
                )
                order_count += 1
                if order_count % 1000 == 0:
                    print(
                        f"Sent {order_count}/{len(records)} order records",
                        flush=True,
                    )
                continue

            if event_type == "forecast":
                forecasts = []
                forecast_end = weather_index + FORECAST_DAYS
                for forecast_date, forecast_row in weather_records[
                    weather_index:forecast_end
                ]:
                    forecast_weather = dict(forecast_row)
                    forecast_weather["date"] = forecast_date.date().isoformat()
                    forecasts.append(normalize_weather_record(forecast_weather))

                forecast_issued_at = event_time.strftime(DATE_FORMAT)
                send_record(
                    producer,
                    WEATHER_PREDICTIONS_TOPIC,
                    key=forecast_issued_at,
                    value={
                        "issued_at": forecast_issued_at,
                        "forecasts": forecasts,
                    },
                    serializer=forecast_serializer,
                )
                continue

            weather_timestamp = event_time.strftime(DATE_FORMAT)
            weather_record = dict(row)
            weather_record["date"] = weather_timestamp
            send_record(
                producer,
                WEATHER_TOPIC,
                key=weather_timestamp,
                value=normalize_weather_record(weather_record),
                serializer=weather_serializer,
            )
    finally:
        remaining = producer.flush(30)
        if remaining:
            raise TimeoutError(
                f"Timed out flushing {remaining} pending Kafka records"
            )

    print(
        f"Finished streaming {len(records)} order records, "
        f"{len(weather_records)} weather records, and full "
        f"{FORECAST_DAYS}-day forecasts",
        flush=True,
    )


if __name__ == "__main__":
    main()
