import json
import logging
import os
import time
from datetime import date, datetime
from functools import lru_cache
from io import BytesIO
from threading import Lock, Thread
from urllib.error import HTTPError
from urllib.request import urlopen

import psycopg
from fastavro import schemaless_reader
from flask import Flask, jsonify
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from psycopg.rows import dict_row
from waitress import serve

app = Flask(__name__)
weather_topic = os.getenv("WEATHER_TOPIC", "weather-london")
forecast_topic = os.getenv("WEATHER_FORECAST_TOPIC", "weather-predictions-london")
predictions_topic = os.getenv("PREDICTIONS_TOPIC", "predictions")
latest = {"weather": None, "prediction": None}
lock = Lock()
forecast_batch = None
sales = {}


def event_time(value):
    return datetime.strptime(value, "%d/%m/%Y %H:%M")


def update_live(topic, record):
    global forecast_batch
    with lock:
        if topic == weather_topic:
            if latest["weather"] is None or event_time(record["date"]) >= event_time(latest["weather"]["date"]):
                latest["weather"] = record
        elif topic == forecast_topic:
            issued = event_time(record["issued_at"])
            days = {}
            for day in record["forecasts"]:
                target = day["date"]
                if "/" in target:  # Older simulator batches used the observation timestamp format.
                    target = event_time(target).date().isoformat()
                days[date.fromisoformat(target).isoformat()] = day
            if forecast_batch is None or issued >= forecast_batch[0]:
                forecast_batch = (issued, days)
        else:
            latest["prediction"] = record
            by_day = sales.setdefault(record["restaurant_id"], {})
            target = record["date"]
            if target not in by_day or record["predicted_at"] >= by_day[target]["predicted_at"]:
                by_day[target] = record
            if len(by_day) > 7:
                del by_day[min(by_day)]


@lru_cache(maxsize=32)
def registered_schema(schema_id):
    registry = os.getenv("KAFKA_SCHEMA_REGISTRY_URL", "http://kafka-schema-registry:8081")
    try:
        with urlopen(f"{registry}/schemas/ids/{schema_id}", timeout=5) as response:
            return json.loads(json.load(response)["schema"])
    except HTTPError as error:
        if error.code == 404:
            raise ValueError(f"Unknown writer schema ID {schema_id}") from error
        raise


@lru_cache(maxsize=1)
def prediction_schema():
    with open(os.getenv("PREDICTIONS_SCHEMA_FILE", "/opt/schemas/prediction.avsc")) as source:
        return json.load(source)


def decode(topic, value):
    if topic == predictions_topic and os.getenv("PREDICTIONS_SCHEMA_ID", "13") == "":
        return schemaless_reader(BytesIO(value), prediction_schema())
    if value.startswith(b"\x00"):
        schema = registered_schema(int.from_bytes(value[1:5], "big"))
        return schemaless_reader(BytesIO(value[5:]), schema)
    raise ValueError("Expected a Schema Registry framed Avro record")


def listen():
    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                weather_topic, forecast_topic, predictions_topic,
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(","),
                group_id=None, enable_auto_commit=False, auto_offset_reset="earliest",
            )
            for message in consumer:
                if message.value is None:
                    continue
                try:
                    record = decode(message.topic, message.value)
                    update_live(message.topic, record)
                except (ValueError, KeyError, IndexError, EOFError, FileNotFoundError) as error:
                    app.logger.warning("Skipping %s partition %s offset %s: %s",
                                       message.topic, message.partition, message.offset, error)
        except (KafkaError, OSError):
            app.logger.exception("Stream unavailable, retrying in five seconds")
        finally:
            if consumer is not None:
                consumer.close()
        time.sleep(5)


@app.get("/")
def dashboard():
    return app.send_static_file("index.html")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/live")
def live():
    with lock:
        days = forecast_batch[1] if forecast_batch else {}
        predictions = [rows[day] for rows in sales.values() for day in days if day in rows]
        return jsonify(**latest, outlook={
            "issued_at": forecast_batch[0].isoformat() if forecast_batch else None,
            "days": days, "predictions": predictions,
        })


@app.get("/api/history/<day>")
def history(day):
    try:
        if date.fromisoformat(day).isoformat() != day:
            raise ValueError
    except ValueError:
        return {"error": "Use a date in YYYY-MM-DD format"}, 400

    try:
        with psycopg.connect(os.getenv("DATABASE_URL", ""), connect_timeout=3,
                             row_factory=dict_row) as conn:
            # Raw order dates use DD/MM/YYYY. Typed timestamps and ISO dates also parse.
            conn.execute("SET LOCAL DateStyle TO 'ISO, DMY'")
            weather = conn.execute(
                """SELECT date, cloud_cover, sunshine, global_radiation, max_temp,
                          mean_temp, min_temp, precipitation, pressure, snow_depth
                   FROM weather WHERE date = %s""", (int(day.replace("-", "")),)
            ).fetchone()
            rows = conn.execute("""
                WITH actual AS (
                    SELECT '1' AS restaurant_id, count(DISTINCT order_number) AS actual_orders
                    FROM "restaurant-1" WHERE order_date::timestamp::date = %(day)s::date
                    HAVING count(*) > 0
                    UNION ALL
                    SELECT '2', count(DISTINCT order_number)
                    FROM "restaurant-2" WHERE order_date::timestamp::date = %(day)s::date
                    HAVING count(*) > 0
                ), forecast AS (
                    SELECT DISTINCT ON (restaurant_id) restaurant_id::text AS id,
                           predicted_orders, predicted_revenue, model_version
                    FROM prediction
                    WHERE date >= %(day)s::date AND date < %(day)s::date + 1
                    ORDER BY restaurant_id, date DESC, model_version DESC NULLS LAST,
                             predicted_orders DESC NULLS LAST, predicted_revenue DESC NULLS LAST
                )
                SELECT coalesce(actual.restaurant_id, forecast.id) AS restaurant_id,
                       actual_orders, predicted_orders, predicted_revenue, model_version,
                       actual_orders - predicted_orders AS difference, NULL AS issued_at
                FROM actual FULL JOIN forecast ON actual.restaurant_id = forecast.id
                ORDER BY restaurant_id
            """, {"day": day}).fetchall()
    except psycopg.Error:
        app.logger.exception("History query failed")
        return {"error": "Historical data unavailable"}, 503

    return {"date": day, "weather": weather, "restaurants": rows}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    Thread(target=listen, daemon=True).start()
    serve(app, host="0.0.0.0", port=8080)
