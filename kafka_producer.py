"""Kafka Producer - Generate Test E-commerce Data"""
import json, time, random
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'raw-events'

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def generate_event():
    return {
        'user_id': random.randint(1, 100),
        'event_type': random.choice(['page_view', 'add_to_cart', 'purchase']),
        'product_id': f"PROD{random.randint(1,50):04d}",
        'category': random.choice(['Electronics', 'Clothing', 'Books']),
        'price': round(random.uniform(100, 5000), 2),
        'quantity': random.randint(1, 5),
        'timestamp': datetime.now().isoformat(),
        'session_id': f"session_{random.randint(1000,9999)}",
        'device': random.choice(['mobile', 'desktop', 'tablet']),
        'location': random.choice(['Mumbai', 'Delhi', 'Bangalore'])
    }

def send_events(producer, num=100):
    print(f"Sending {num} events to Kafka...")
    for i in range(num):
        event = generate_event()
        producer.send(TOPIC, value=event)
        if (i+1) % 10 == 0:
            print(f"✓ Sent {i+1}/{num} events")
        time.sleep(0.1)
    producer.flush()
    print(f"✓ Completed! Sent {num} events")

if __name__ == "__main__":
    print("Kafka Event Producer")
    print("=" * 50)
    producer = create_producer()
    print(f"✓ Connected to Kafka: {KAFKA_BROKER}")
    num_events = input("Number of events to send (default 100): ")
    num_events = int(num_events) if num_events else 100
    send_events(producer, num_events)
    producer.close()
