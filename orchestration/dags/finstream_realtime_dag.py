from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'aditya',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='finstream_realtime_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Generate Data
    gen_data = BashOperator(
        task_id='generate_data',
        bash_command='cd /opt/airflow && export PYTHONPATH=/opt/airflow && python src/producer/main.py'
    )

    # Task 2: Bronze Ingestion
    bronze = BashOperator(
        task_id='bronze_ingestion',
        bash_command='cd /opt/airflow && export PYTHONPATH=/opt/airflow && python src/spark_jobs/streaming/bronze_ingestion.py'
    )

    # Task 3: Silver Cleansing
    silver = BashOperator(
        task_id='silver_cleansing',
        bash_command='cd /opt/airflow && export PYTHONPATH=/opt/airflow && python src/spark_jobs/streaming/silver_ingestion.py'
    )

    # Task 4: dbt Gold Transformation (Pointed to gold_analytics!)
    gold = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/gold_analytics && dbt run --profiles-dir /opt/airflow/gold_analytics'
    )

    # Task 5: Great Expectations Validation
    quality = BashOperator(
        task_id='data_quality_check',
        bash_command='cd /opt/airflow && export PYTHONPATH=/opt/airflow && python src/data_quality/run_checkpoint.py'
    )
    
    celebrate = BashOperator(
        task_id='deployment_success',
        bash_command='echo "CI/CD Pipeline is fully operational!"'
    )

    # Update the sequence to include the new task
    gen_data >> bronze >> silver >> gold >> quality >> celebrate

    
