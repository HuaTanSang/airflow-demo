from confluent_kafka import Consumer
import os
import json
import pandas as pd


def consumer_func():
    consumer = Consumer({
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'group.id': 'airflow-consumer',
        'auto.offset.reset': 'earliest'
    })
    topic = os.getenv('TOPIC', 'faker-topic')
    raw_path = os.getenv('RAW_DATA_PATH', '/opt/airflow/data/raw_data.csv')  # ↔️ Đồng nhất biến đường dẫn

    consumer.subscribe([topic])
    print("Starting consumer...")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue

        record = json.loads(msg.value().decode('utf-8'))
        df = pd.DataFrame([record])
        header = not os.path.exists(raw_path)            # ➕ Nếu file chưa tồn tại, thêm header
        df.to_csv(raw_path, mode='a', header=header, index=False)  # ➕ Append dữ liệu
        print(f"Consumed and wrote: {record}")

if __name__ == "__main__":
    consumer_func()