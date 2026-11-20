import csv
import json
import os
import time
from datetime import datetime
from pathlib import Path

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError


DATE_FORMAT = "%d/%m/%Y %H:%M"
DATA_DIRECTORY = Path(__file__).resolve().parent
INPUTS = (
    ("restaurant-1", "restaurant-1-orders-streaming.csv"),
    ("restaurant-2", "restaurant-2-orders-streaming.csv"),
)


def load_records():
    records = []
    for source, filename in INPUTS:
        path = DATA_DIRECTORY / filename
        with path.open(newline="", encoding="utf-8-sig") as csv_file:
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


def order_key(source, row):
    key_field = "Order Number" if source == "restaurant-1" else "Order ID"
    return row.get(key_field)


def main():
    records = load_records()
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
    first_event_time = records[0][0]
    started_at = time.monotonic()
    print(
        f"Streaming {len(records)} records to {topic}; "
        f"event range {first_event_time} through {records[-1][0]} "
        "(1 event-day = 60 seconds)",
        flush=True,
    )

    try:
        for index, (event_time, source, row) in enumerate(records, start=1):
            elapsed_event_days = (event_time - first_event_time).total_seconds() / 86400
            target_elapsed = elapsed_event_days * 60
            wait_seconds = target_elapsed - (time.monotonic() - started_at)
            if wait_seconds > 0:
                time.sleep(wait_seconds)

            producer.send(
                topic,
                key=order_key(source, row),
                value={"source": source, "record": row},
            ).get(timeout=30)
            if index % 1000 == 0:
                print(f"Sent {index}/{len(records)} records", flush=True)
    finally:
        producer.flush()
        producer.close()

    print(f"Finished streaming {len(records)} records to {topic}", flush=True)


if __name__ == "__main__":
    main()
