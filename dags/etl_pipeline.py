# dags/mlops_seminar_dag.py
import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.email import EmailOperator
from sqlalchemy import create_engine

# Đường dẫn file
RAW_DATA_PATH = "/opt/airflow/data/raw_data.csv"
CLEAN_DATA_PATH = "/opt/airflow/data/clean_data.csv"

# Cấu hình DB
HOST = 'recalls_db'
DB = 'recalls_db'
USER = 'admin'
PWD = 'admin'
PORT = 5432

# Thông số mặc định
default_args = {
    'start_date': datetime(2025, 5, 1),
    'catchup': False,
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'email': ['huatansang2004@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True
}

# Tạo DAG
with DAG(
    'mlops_seminar',
    default_args=default_args,
    schedule_interval="0 * * * *",
    description='MLOps Seminars',
) as dag:

    # Task 1: Kiểm tra có dữ liệu mới hay không
    def check_new_data(**kwargs):
        if not os.path.exists(RAW_DATA_PATH):
            return False
        df = pd.read_csv(RAW_DATA_PATH)
        return not df.empty

    check_task = ShortCircuitOperator(
        task_id='check_new_data',
        python_callable=check_new_data,
    )

    # Task 2: Chuyển đổi dữ liệu
    def transform_data():
        df = pd.read_csv(RAW_DATA_PATH)
        df[['first_name', 'last_name']] = df['name'].apply(lambda x: pd.Series(x.split(" ", 1)))
        df[['street', 'county']] = df['address'].apply(lambda x: pd.Series(x.split("; ", 1)))
        clean_df = pd.DataFrame({
            'id': df['time'].astype(str),
            'first_name': df['first_name'],
            'last_name': df['last_name'],
            'street': df['street'],
            'county': df['county'],
            'email': df['email'],
            'phone_number': df['phone_number'],
            'time_stamp': df['time']
        })

        # Xoá dữ liệu gốc
        pd.read_csv(RAW_DATA_PATH, nrows=0).to_csv(RAW_DATA_PATH, index=False)
        clean_df.to_csv(CLEAN_DATA_PATH, index=False)
        print(f"Transformed {len(clean_df)} records")

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    # Task 3: Load vào DB
    def load_to_db():
        df = pd.read_csv(CLEAN_DATA_PATH)
        df['ingested_at'] = datetime.now()
        engine = create_engine(f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")
        df.to_sql('recalls', con=engine, if_exists='append', index=False)

        # Xoá file sau khi load
        pd.read_csv(CLEAN_DATA_PATH, nrows=0).to_csv(CLEAN_DATA_PATH, index=False)
        print(f"Loaded {len(df)} rows into DB")

    load_task = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_db,
    )

    # Task 4: Gửi email thông báo sau khi load xong
    send_email = EmailOperator(
        task_id='send_email_task',
        to='huatansang2004@gmail.com',
        subject='✅ Load dữ liệu thành công',
        html_content="""
        <h3>Dữ liệu đã được load vào database thành công!</h3>
        """,
    )
    
    check_task >> transform_task >> load_task >> send_email
