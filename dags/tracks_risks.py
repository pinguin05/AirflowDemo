from datetime import timedelta, datetime
from textwrap import dedent

from pipeline.module_A import load_gpx, parse_simple_features

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
    'ML_tracks_risks',
    default_args=default_args,
    description='',
    schedule=None,
    start_date=datetime.now(),
    tags=['ML', 'gpx'],
) as dag:
    gpx_loader = PythonOperator(
        task_id='load_gpx_tracks',
        python_callable=load_gpx,
        dag=dag
    )
    simple_parser = PythonOperator(
        task_id='parse_simple_features',
        python_callable=parse_simple_features,
        dag=dag
    )

    gpx_loader >> simple_parser