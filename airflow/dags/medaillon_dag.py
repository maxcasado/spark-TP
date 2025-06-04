from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

# Connexion S3 (MinIO)
MINIO_CONN_ID = 'minio_conn'
BRONZE_BUCKET = 'bronze'
SILVER_BUCKET = 'silver'

def read_from_bronze():
    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    files = s3.list_keys(bucket_name=BRONZE_BUCKET)
    print(f"Fichiers dans bronze: {files}")

def run_spark_job():
    # Exemple basique: appelle un script spark-submit dans spark/jobs/
    # Adapte le chemin si besoin
    cmd = [
        "/opt/spark/bin/spark-submit",
        "/opt/spark/jobs/transform.py"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Spark job failed: {result.stderr}")

def write_to_silver():
    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    # Ici tu écris un fichier (exemple simple)
    s3.load_string(
        string_data="Données transformées",
        key="output/result.txt",
        bucket_name=SILVER_BUCKET,
        replace=True
    )
    print("Fichier écrit dans silver")

with DAG(dag_id="medaillon_etl", start_date=datetime(2025,6,1), schedule_interval="@daily", catchup=False) as dag:
    t1 = PythonOperator(task_id="list_bronze_files", python_callable=read_from_bronze)
    t2 = PythonOperator(task_id="spark_transform", python_callable=run_spark_job)
    t3 = PythonOperator(task_id="write_silver", python_callable=write_to_silver)

    t1 >> t2 >> t3
