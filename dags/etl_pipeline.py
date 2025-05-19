# dags/mlops_seminar_dag.py
import os
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from sqlalchemy import create_engine

RAW_DATA_PATH = "/opt/airflow/data/raw_data.csv"
CLEAN_DATA_PATH = "/opt/airflow/data/clean_data.csv"

HOST = 'recalls_db'
DB = 'recalls_db'
USER = 'admin'
PWD = 'admin'
PORT = 5432

default_args = {
    'start_date': datetime(2025, 5, 1),
    'catchup': False
}

dag = DAG(
    'mlops_seminar',
    default_args=default_args,
    schedule_interval="* * * * *"
)

def check_new_data(**kwargs):
    if not os.path.exists(RAW_DATA_PATH):
        return False
    df = pd.read_csv(RAW_DATA_PATH)
    return not df.empty

check_task = ShortCircuitOperator(
    task_id='check_new_data',
    python_callable=check_new_data,
    dag=dag
)

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

    # Xoa data da doc 
    df = pd.read_csv(RAW_DATA_PATH, nrows=0)
    df.to_csv(RAW_DATA_PATH, index=False)

    clean_df.to_csv(CLEAN_DATA_PATH, index=False)
    print(f"Transformed {len(clean_df)} records")

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

def load_to_db():
    df = pd.read_csv(CLEAN_DATA_PATH)
    df['ingested_at'] = datetime.now()
    engine = create_engine(f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")
    df.to_sql('recalls', con=engine, if_exists='append', index=False)

    # Xoa file sau khi da load xong vao db 
    df = pd.read_csv(CLEAN_DATA_PATH, nrows=0)
    df.to_csv(CLEAN_DATA_PATH, index=False)

    print(f"Loaded {len(df)} rows into DB")

load_task = PythonOperator(
    task_id='load_to_db',
    python_callable=load_to_db,
    dag=dag
)

check_task >> transform_task >> load_task
