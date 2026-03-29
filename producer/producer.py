import json
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "game_events"

PLATFORMS = ["ios", "android", "web"]
COUNTRIES = ["IN", "US", "DE", "BR", "JP", "GB", "FR", "CA"]
GAME_VERSIONS = ["3.1.0", "3.1.1", "3.2.0"]
EVENT_TYPES = ["session_start", "level_start", "level_complete", "purchase", "ad_impression"]

PURCHASE_ITEMS = [
    {"item": "starter_pack",   "price": 0.99},
    {"item": "gem_pack_small", "price": 1.99},
    {"item": "gem_pack_large", "price": 4.99},
    {"item": "battle_pass",    "price": 9.99},
    {"item": "no_ads",         "price": 2.99},
]

LOCALE_BY_COUNTRY = {
    "IN": "en_IN", "US": "en_US", "DE": "de_DE",
    "BR": "pt_BR", "JP": "ja_JP", "GB": "en_GB",
    "FR": "fr_FR", "CA": "en_CA",
}

fakers = {country: Faker(locale) for country, locale in LOCALE_BY_COUNTRY.items()}


def make_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("Connected to Kafka")
            return producer
        except NoBrokersAvailable:
            print("Waiting for Kafka...")
            time.sleep(3)


def make_user():
    country = random.choice(COUNTRIES)
    fake = fakers[country]
    return {
        "user_id":      str(uuid4()),
        "username":     fake.user_name(),
        "device_model": fake.user_agent(),
        "platform":     random.choice(PLATFORMS),
        "country":      country,
        "game_version": random.choice(GAME_VERSIONS),
        "level":        random.randint(1, 50),
    }


def make_event(user):
    event_name = random.choice(EVENT_TYPES)
    event = {
        "event_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "user_id":         user["user_id"],
        "username":        user["username"],
        "session_id":      str(uuid4()),
        "platform":        user["platform"],
        "country":         user["country"],
        "game_version":    user["game_version"],
        "level":           user["level"],
        "event_name":      event_name,
        "device_model":    user["device_model"],
    }

    if event_name == "purchase":
        item = random.choice(PURCHASE_ITEMS)
        event["event_params"] = json.dumps({
            "item_id":  item["item"],
            "value":    item["price"],
            "currency": "USD",
        })
    elif event_name in ("level_start", "level_complete"):
        event["event_params"] = json.dumps({
            "level_number": user["level"],
            "difficulty":   random.choice(["easy", "normal", "hard"]),
        })
    else:
        event["event_params"] = json.dumps({})

    return event


def make_malformed_event():
    malformed_type = random.choice(["missing_user_id", "bad_timestamp", "empty"])

    if malformed_type == "missing_user_id":
        return {
            "event_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "event_name":      "level_start",
            "session_id":      str(uuid4()),
        }
    elif malformed_type == "bad_timestamp":
        return {
            "event_timestamp": "not-a-date",
            "event_name":      "session_start",
            "user_id":         str(uuid4()),
            "session_id":      str(uuid4()),
        }
    else:
        return {}


def run():
    producer = make_producer()
    users = [make_user() for _ in range(20)]

    sent = 0
    while True:
        user = random.choice(users)

        if random.random() < 0.05:
            event = make_malformed_event()
        else:
            event = make_event(user)

        producer.send(TOPIC, value=event)
        sent += 1

        if sent % 100 == 0:
            producer.flush()
            print(f"Sent {sent} events")

        time.sleep(random.uniform(0.05, 0.2))


if __name__ == "__main__":
    run()