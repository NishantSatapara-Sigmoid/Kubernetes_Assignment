from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from fetch_data import fetch_info
default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 23),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
     "retry_delay": timedelta(seconds=5),
}

dag = DAG("Docker_assignment",default_args=default_args,schedule_interval='@daily',catchup=False,template_searchpath=['/usr/local/airflow/sql_files'])

t1 = PostgresOperator(task_id='connect_db_and_add_data', postgres_conn_id="docker_assignment", sql="Add_data.sql",dag=dag)

t2 = PythonOperator(task_id='fetching_data_from_PostgresDB', python_callable=fetch_info, dag=dag)

t1 >> t2