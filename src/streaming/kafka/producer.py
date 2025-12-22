import json
import time
import uuid
import random
from datetime import datetime, timezone

import yaml
from kafka import KafkaProducer


class TransactionProducer:
    def __init__(self, config_path: str):
        with open(config_path, "r") as f:
            kafka_config = yaml.safe_load(f)["kafka"]

        self.topic = kafka_config["topic"]

        producer_config = {
            "bootstrap_servers": kafka_config["bootstrap_servers"],
            "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
            "acks": kafka_config["acks"],
            "retries": kafka_config["retries"],
            "linger_ms": kafka_config["linger_ms"],
            "batch_size": kafka_config["batch_size"],
        }

        if "compression_type" in kafka_config:
            producer_config["compression_type"] = kafka_config["compression_type"]

        self.producer = KafkaProducer(**producer_config)

    def _generate_transaction(self) -> dict:
        amount = round(random.uniform(1, 5000), 2)
        country = random.choice(["IN", "US", "UK", "DE", "SG"])
        is_international = country != "IN"

        return {
            "transaction_id": str(uuid.uuid4()),
            "user_id": f"user_{random.randint(1, 50000)}",
            "wallet_id": f"wallet_{random.randint(1, 50000)}",
            "amount": amount,
            "currency": "INR",
            "merchant_id": f"merchant_{random.randint(1, 2000)}",
            "merchant_category": random.choice(
                ["grocery", "electronics", "travel", "food", "fuel"]
            ),
            "transaction_type": random.choice(
                ["p2p", "merchant_payment", "bill_payment"]
            ),
            "device_id": f"device_{random.randint(1, 100000)}",
            "ip_address": f"192.168.{random.randint(0, 255)}.{random.randint(0, 255)}",
            "country": country,
            "is_international": is_international,
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def produce(self, rate_per_second: int = 300):
        interval = 1.0 / rate_per_second

        while True:
            event = self._generate_transaction()
            self.producer.send(self.topic, event)
            print(f"Produced event {event['transaction_id']}")
            time.sleep(interval)


if __name__ == "__main__":
    producer = TransactionProducer("config/kafka.yaml")
    producer.produce(rate_per_second=300)