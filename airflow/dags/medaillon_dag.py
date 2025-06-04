from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='medaillon_dag',
    default_args=default_args,
    description='Pipeline medaillon Spark -> MinIO',
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'minio', 'medaillon']
) as dag:

    clean_contracts = BashOperator(
        task_id='clean_contracts_job',
        bash_command='spark-submit /opt/spark/jobs/clean_contracts_job.py'
    )

    clean_etabs = BashOperator(
        task_id='clean_etablissement_job',
        bash_command='spark-submit /opt/spark/jobs/clean_etablissement_job.py'
    )

    join_gold = BashOperator(
        task_id='join_public_contracts_job',
        bash_command='spark-submit /opt/spark/jobs/join_public_contracts_job.py'
    )

    [clean_contracts, clean_etabs] >> join_gold
