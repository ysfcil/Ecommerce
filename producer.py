#!/usr/bin/env python3
import json
import os
import random
import uuid
import time
import sys
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer

# Initialize Faker
fake = Faker()

# Pre-define realistic product categories and devices for our bias
DEVICES = ["mobile", "desktop", "tablet"]
OS_MAP = {
    "mobile": ["iOS", "Android"],
    "desktop": ["Windows", "macOS", "Linux"],
    "tablet": ["iPadOS", "Android"]
}
BROWSERS = ["Safari", "Chrome", "Firefox", "Edge"]
CATEGORIES = {
    "Electronics": ["Audio", "Computers", "Accessories"],
    "Apparel": ["Men's", "Women's", "Shoes"],
    "Home": ["Furniture", "Decor", "Kitchen"]
}

# Your custom bias logic
CATEGORY_WEIGHTS = {
    "desktop": [0.60, 0.10, 0.30],
    "mobile": [0.10, 0.60, 0.30],
    "tablet": [0.30, 0.30, 0.40]
}

# --- THE FUNNEL STATE DICTIONARY ---
# This will temporarily hold users who are actively clicking through the site
active_sessions = {}

# Metrics tracking
metrics = {
    "total_events": 0,
    "total_bytes": 0,
    "start_time": None,
    "end_time": None,
    "events_by_type": {},
}


def generate_ecommerce_event():
    event_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1. Decide: Progress an existing active session, or start a new one?
    # We give a 40% chance to progress an existing session (if any exist) to simulate concurrent traffic
    if active_sessions and random.random() < 0.40:

        # Pick a random active user session
        session_id = random.choice(list(active_sessions.keys()))
        session_data = active_sessions[session_id]

        # Pull their exact user and product data from memory to maintain the sequence
        user = session_data["user"]
        product = session_data["product"]
        event_type = session_data["next_step"]

        # Determine what happens NEXT in their funnel
        if event_type == "add_to_cart":
            # Decide quantity now that they are adding to cart
            product["quantity"] = random.randint(1, 3)

            # Give them a 30% chance to eventually buy, otherwise they abandon the cart
            if random.random() < 0.30:
                active_sessions[session_id]["next_step"] = "purchase"
            else:
                del active_sessions[session_id]  # Cart abandoned, remove from memory

        elif event_type == "purchase":
            # The funnel is complete! Remove them from active memory.
            del active_sessions[session_id]

    else:
        # 2. Start a completely NEW session
        session_id = str(uuid.uuid4())
        event_type = "view_item"  # New sessions ALWAYS start with a view

        # Build the User Object
        device_type = random.choice(DEVICES)
        user = {
            "user_id": f"usr_{random.randint(10000, 99999)}",
            "name": fake.name(),
            "email": fake.email(),
            "location": {
                "country": "United States",
                "state": fake.state(),
                "city": fake.city()
            },
            "device": {
                "type": device_type,
                "os": random.choice(OS_MAP[device_type]),
                "browser": random.choice(BROWSERS)
            }
        }

        # Build the Product Object based on device bias
        category = random.choices(
            list(CATEGORIES.keys()),
            weights=CATEGORY_WEIGHTS[device_type],
            k=1
        )[0]
        sub_category = random.choice(CATEGORIES[category])
        product = {
            "product_id": f"prod_{random.randint(1000, 9999)}",
            "title": fake.catch_phrase(),
            "category": category,
            "sub_category": sub_category,
            "price": round(random.uniform(9.99, 499.99), 2),
            "quantity": 1  # Default view quantity
        }

        # Will this new user continue down the funnel later?
        # Let's give a 50% chance they eventually add this item to their cart
        if random.random() < 0.50:
            active_sessions[session_id] = {
                "user": user,
                "product": product,
                "next_step": "add_to_cart"  # Queue up the next step
            }

    # 3. Assemble the final JSON payload
    payload = {
        "event_id": event_id,
        "session_id": session_id,
        "timestamp": timestamp,
        "event_type": event_type,
        "user_id": user.get("user_id"),
        "product_id": product.get("product_id"),
        "device_id": user["device"]["type"],
        "user": user,
        "product": product
    }

    return payload


def format_bytes(bytes_val):
    """Convert bytes to human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} TB"


def save_metrics(metrics_dict):
    """Save metrics to file for external monitoring"""
    try:
        with open('/app/producer_metrics.json', 'w') as f:
            json.dump(metrics_dict, f, indent=2)
    except Exception as e:
        print(f"Warning: Could not save metrics file: {e}")


if __name__ == "__main__":
    kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    max_events = int(os.environ.get("MAX_EVENTS", "1000"))

    print(
        f"Connecting to Kafka at {kafka_bootstrap} "
        f"and starting data stream... (Press Ctrl+C to stop)\n"
    )

    try:
        # Initialize the Kafka Producer
        producer = KafkaProducer(
            bootstrap_servers=[kafka_bootstrap],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=3
        )
    except Exception as e:
        print(f"Failed to connect to Kafka broker. Is it running? Error: {e}")
        exit(1)

    metrics["start_time"] = datetime.now(timezone.utc).isoformat()

    try:
        for i in range(max_events):
            event_data = generate_ecommerce_event()

            # Serialize to JSON
            event_json = json.dumps(event_data)
            event_bytes = event_json.encode("utf-8")

            # Send the JSON payload to Kafka
            producer.send("ecommerce_clickstream", event_data)

            # Update metrics
            metrics["total_events"] += 1
            metrics["total_bytes"] += len(event_bytes)
            event_type = event_data['event_type']
            if event_type not in metrics["events_by_type"]:
                metrics["events_by_type"][event_type] = 0
            metrics["events_by_type"][event_type] += 1

            # Print with metrics every 10 events
            if (i + 1) % 10 == 0:
                throughput = metrics["total_bytes"] / (time.time() - float(datetime.fromisoformat(metrics["start_time"]).timestamp()))
                print(
                    f"[{i+1}/{max_events}] Events: {metrics['total_events']} | "
                    f"Data: {format_bytes(metrics['total_bytes'])} | "
                    f"Throughput: {format_bytes(throughput)}/sec | "
                    f"View: {metrics['events_by_type'].get('view_item', 0)} | "
                    f"Cart: {metrics['events_by_type'].get('add_to_cart', 0)} | "
                    f"Purchase: {metrics['events_by_type'].get('purchase', 0)}"
                )

            # Pause briefly to simulate real-time traffic
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nData stream stopped.")
    finally:
        metrics["end_time"] = datetime.now(timezone.utc).isoformat()
        
        # Ensure all queued messages are sent before shutting down
        producer.flush()
        producer.close()
        
        # Calculate final statistics
        if metrics["start_time"] and metrics["end_time"]:
            start = datetime.fromisoformat(metrics["start_time"])
            end = datetime.fromisoformat(metrics["end_time"])
            duration = (end - start).total_seconds()
            if duration > 0:
                throughput = metrics["total_bytes"] / duration
                metrics["throughput_bytes_per_sec"] = throughput
                metrics["duration_seconds"] = duration
        
        save_metrics(metrics)
        
        print("\n" + "="*80)
        print("STREAMING SUMMARY")
        print("="*80)
        print(f"Total Events: {metrics['total_events']}")
        print(f"Total Data: {format_bytes(metrics['total_bytes'])}")
        print(f"Events by Type: {metrics['events_by_type']}")
        if "throughput_bytes_per_sec" in metrics:
            print(f"Throughput: {format_bytes(metrics['throughput_bytes_per_sec'])}/sec")
            print(f"Duration: {metrics['duration_seconds']:.1f} seconds")
        print("="*80)
        print("Kafka Producer closed.")
