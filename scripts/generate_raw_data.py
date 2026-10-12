"""Step 1: generate raw sneaker events for a given date and upload to bronze (JSON lines)."""
import json
import os
import random
import sys
import uuid
from datetime import datetime, timezone, timedelta

import boto3
from botocore.client import Config

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
S3_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://s3:9000")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def generate_sneaker_events(target_date: str, count: int = 300):
    catalog = [
        {"brand": "Nike", "model": "Air Jordan 1 Retro", "base_price": 18000},
        {"brand": "Nike", "model": "Dunk Low", "base_price": 13000},
        {"brand": "Adidas", "model": "Samba OG", "base_price": 12000},
        {"brand": "Adidas", "model": "Campus 00s", "base_price": 11000},
        {"brand": "New Balance", "model": "1906R", "base_price": 19500},
        {"brand": "New Balance", "model": "550", "base_price": 14000},
        {"brand": "Asics", "model": "Gel-Kayano 14", "base_price": 17000},
        {"brand": "Salomon", "model": "XT-6", "base_price": 21000},
    ]
    sizes = [36.0, 37.5, 38.0, 39.0, 40.0, 41.0, 42.0, 42.5, 43.0, 44.0, 45.0, 46.0]
    channels = ["mobile_app", "web_store", "flagship_retail"]
    customers = [f"cust_{i}" for i in range(101, 200)]
    day_start = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    events = []
    for _ in range(count):
        item = random.choice(catalog)
        discount = random.choice([0, 500, 1000, 1500])
        event_time = day_start + timedelta(seconds=random.randint(0, 86399))
        events.append({
            "order_id": str(uuid.uuid4()),
            "customer_id": random.choice(customers),
            "brand": item["brand"],
            "model": item["model"],
            "size_eu": float(random.choice(sizes)),
            "channel": random.choice(channels),
            "price": float(item["base_price"]),
            "discount_amount": float(discount),
            "final_price": float(item["base_price"] - discount),
            "event_type": random.choices(["PURCHASE", "RETURN"], weights=[0.88, 0.12])[0],
            "event_time": event_time.isoformat(),
        })
    return events


def upload_raw_data(target_date: str):
    s3 = get_s3_client()
    # Fixed file name per date -> re-running the task overwrites instead of duplicating
    s3_key = f"raw/date={target_date}/sneaker_orders.json"
    events = generate_sneaker_events(target_date)
    body = "\n".join(json.dumps(e) for e in events).encode("utf-8")
    s3.put_object(Bucket="bronze", Key=s3_key, Body=body, ContentType="application/x-ndjson")
    print(f"Uploaded {len(events)} events to s3://bronze/{s3_key}")


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    upload_raw_data(date_arg)