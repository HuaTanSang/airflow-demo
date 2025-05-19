from confluent_kafka import Producer
from faker import Faker
import os
import json
import time

def producer_func():
    fake = Faker()
    producer = Producer({
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    })

    topic = os.getenv('TOPIC', 'faker-topic')
    raw_path = os.getenv('RAW_DATA_PATH', '/opt/airflow/data/raw_data.csv')  # ↔️ Nhận RAW_DATA_PATH từ ENV

    while True:
        data = {
            "time_stamp": fake.iso8601(),          # ↔️ Sửa: dùng `time_stamp` nhất quán với DAG
            "name": f"{fake.first_name()} {fake.last_name()}",
            "address": fake.address().replace("\n", "; "),
            "email": fake.email(),
            "phone_number": fake.phone_number()     # ↔️ Sửa: chính xác `phone_number`
        }
        producer.produce(topic, json.dumps(data).encode('utf-8'))  # ↔️ Giữ nguyên logic gửi message
        producer.flush()
        print(f"Produced: {data}")
        time.sleep(1)

if __name__ == "__main__":
    producer_func()