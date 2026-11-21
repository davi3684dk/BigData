import csv
import io
import json
import os
import time
import zipfile
from contextlib import contextmanager
from datetime import datetime, time as datetime_time
from pathlib import Path

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError


DATE_FORMAT = "%d/%m/%Y %H:%M"
WEATHER_DATE_FORMAT = "%Y%m%d"
WEATHER_FILENAME = "london_weather.csv"
WEATHER_TOPIC = "weather-london"
WEATHER_PREDICTIONS_TOPIC = "weather-predictions-london"
FORECAST_DAYS = 7
DATA_DIRECTORY = Path(__file__).resolve().parent
INPUT_ARCHIVE = "streaming-inputs.zip"
INPUTS = (
    ("restaurant-1", "restaurant-1-orders-streaming.csv"),
    ("restaurant-2", "restaurant-2-orders-streaming.csv"),
)


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


def main():
    records = load_records()
    weather_records = load_weather_records()
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC", "restaurant-orders")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        acks="all",
        retries=5,
        key_serializer=lambda key: key.encode("utf-8") if key is not None else None,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode(
            "utf-8"
        ),
    )
    first_order_time = records[0][0]
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
    events.sort(key=lambda event: (event[0], event[1], event[2]))
    simulation_start = first_order_time
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
                producer.send(
                    topic,
                    key=order_key(source, row),
                    value={"source": source, "record": row},
                ).get(timeout=30)
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
                    forecast_timestamp = datetime.combine(
                        forecast_date.date(),
                        datetime.min.time(),
                    ).strftime(DATE_FORMAT)
                    forecast_weather = dict(forecast_row)
                    forecast_weather["date"] = forecast_timestamp
                    forecasts.append(forecast_weather)

                forecast_issued_at = event_time.strftime(DATE_FORMAT)
                producer.send(
                    WEATHER_PREDICTIONS_TOPIC,
                    key=forecast_issued_at,
                    value={
                        "issued_at": forecast_issued_at,
                        "forecasts": forecasts,
                    },
                ).get(timeout=30)
                continue

            weather_timestamp = event_time.strftime(DATE_FORMAT)
            weather_record = dict(row)
            weather_record["date"] = weather_timestamp
            producer.send(
                WEATHER_TOPIC,
                key=weather_timestamp,
                value=weather_record,
            ).get(timeout=30)
    finally:
        producer.flush()
        producer.close()

    print(
        f"Finished streaming {len(records)} order records, "
        f"{len(weather_records)} weather records, and full "
        f"{FORECAST_DAYS}-day forecasts",
        flush=True,
    )


if __name__ == "__main__":
    main()
