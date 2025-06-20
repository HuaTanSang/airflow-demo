from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.email import EmailOperator


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay':timedelta(seconds=5),
    'email':['huatansang2004@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':True
}
with DAG(
    'test_send_mail',
    start_date=datetime(2025, 5, 1),
    schedule_interval='@once',
    default_args=default_args
) as dag:
 send_email = EmailOperator(
        task_id='send_email_task',
        to='huatansang2004@gmail.com',
        subject='testtt',
        html_content="""
        <h3>helooo</h3>
        """,
    )
 send_email




