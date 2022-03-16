from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from task1 import write_into_csv
from task2_3 import create_insert_daily_weather_info

# instantiating the dag
with DAG(
        'weather_info',
        default_args={
            'owner': 'danish',
            'start_date': datetime(2022, 3, 14),
            'email': ['danish99011@gmail.com'],
            'email_on_failure': True,
            'email_on_retry': True,
            'retries': 1,
            # 'retries':2,
            # 'retry_delay': timedelta(minutes=5)
        },
        description='A simple dag which gives daily weather report',
        schedule_interval='0 6 * * *'
) as dag:
    # dag tasks
    t1 = PythonOperator(task_id='write_into_csv', python_callable=write_into_csv)
    t2 = PythonOperator(task_id='insert_into_database', python_callable=create_insert_daily_weather_info)

# order of tasks
t1 >> t2
