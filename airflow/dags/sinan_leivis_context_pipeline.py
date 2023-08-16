# [start Steps:]
# Verifica se algum arquivo foi adicionado ao bucket raw_leivisproject/sinan_leivis
# Se sim:
#   cria bucket temporário processing_leivisproject/sinan_leivis e sincroniza os arquivos
#   faz operações de conversão de tipos
#   uni os arquivos granulares em um
#   realiza limpeza e correção dos dados
#   cria banco de dados no big query e tabela sinan_leivis, se não existir
#   persiste os dados na tabela
#   encerra dag
# [end Steps]

import os
from os import getenv
import pandas as pd
import numpy as np
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSSynchronizeBucketsOperator, \
    GCSListObjectsOperator, GCSDeleteBucketOperator, GCSDeleteObjectsOperator

from airflow.providers.google.cloud.transfers.local_to_gcs import GCSHook

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCheckOperator, \
    BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, \
    DataprocDeleteClusterOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator

os.environ["GCP_PROJECT_ID"] = GCP_PROJECT_ID = Variable.get('GCP_PROJECT_ID')
os.environ["GCP_CONN_ID"] = GCP_CONN_ID = Variable.get('GCP_CONN_ID')
os.environ["REGION"] = REGION = Variable.get('REGION')
os.environ["LOCATION"] = LOCATION = Variable.get('LOCATION')
os.environ["RAW_BUCKET"] = RAW_BUCKET = Variable.get('RAW_BUCKET')
os.environ["PROCESSING_BUCKET"] = PROCESSING_BUCKET = Variable.get('PROCESSING_BUCKET')
os.environ["TRUSTED_BUCKET"] = TRUSTED_BUCKET = Variable.get('TRUSTED_BUCKET')
os.environ["BQ_DATASET_NAME"] = BQ_DATASET_NAME = Variable.get('BQ_DATASET_NAME')
os.environ["BQ_SINAN_TABLE_NAME"] = BQ_SINAN_TABLE_NAME = Variable.get('BQ_SINAN_TABLE_NAME')
os.environ["JAVA_HOME"] = Variable.get('JAVA_HOME')

default_args = {
    'owner': 'aulafia',
    'depends_on_past': False,
    'email': ['flavialopesads@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# [START python code]

# [START dag]
dag = DAG(
    dag_id='sinan_leivis_context_pipeline',
    default_args=default_args,
    start_date=datetime(year=2023, month=7, day=31),
    schedule_interval='@daily',
    catchup=False,
    tags=['PROCESSING', 'CONTEXT']
)
# [END dag]

# [START set_tasks]
start_context = EmptyOperator(task_id='start_context')

bq_create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='bq_create_dataset',
    project_id=GCP_PROJECT_ID,
    dataset_id=BQ_DATASET_NAME,
    location=LOCATION,
    gcp_conn_id=GCP_CONN_ID,
    dag=dag
)

bq_create_table = BigQueryCreateEmptyTableOperator(
    task_id='bq_create_table',
    project_id=GCP_PROJECT_ID,
    dataset_id=BQ_DATASET_NAME,
    table_id=BQ_SINAN_TABLE_NAME,
    location=LOCATION,
    gcp_conn_id=GCP_CONN_ID,
    dag=dag
)

check_bq_table = BigQueryCheckOperator(
    task_id='check_bq_table',
    sql="SELECT COUNT(*) FROM {BQ_DATASET_NAME}.{BQ_SINAN_TABLE_NAME}",
    use_legacy_sql=False,
    location='us',
    gcp_conn_id=GCP_CONN_ID,
    dag=dag
)

# [start SPARK - usar spark na parte de mineração/outros]
DATAPROC_CLUSTER_NAME = getenv('DATAPROC_CLUSTER_NAME', 'leivis-cluster')
PYSPARK_URI = getenv('PYSPARK_URI', '')
dp_cluster_config = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {
            "boot_disk_type": "pd_standard",
            "boot_disk_size_gb": 100,
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {
                "boot_disk_type": "pd_standard",
                "boot_disk_size_gb": 100,
            },
        }
    }
}

create_dataproc_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    project_id=GCP_PROJECT_ID,
    cluster_name=DATAPROC_CLUSTER_NAME,
    cluster_config=dp_cluster_config,
    region=REGION,
    use_if_exists=True,
    gcp_conn_id=GCP_CONN_ID,
    dag=dag
)

pyspark_job = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

pyspark_job_submit = DataprocSubmitJobOperator(
    task_id='pyspark_job_submit',
    project_id=GCP_PROJECT_ID,
    region=REGION,
    job=pyspark_job,
    asynchronous=True,
    gcp_conn_id=GCP_CONN_ID,
    dag=dag
)

dataproc_job_sensor = DataprocJobSensor(
    task_id='dataproc_job_sensor',
    project_id=GCP_PROJECT_ID,
    region=REGION,
    dataproc_job_id={"{{task_instance.xcom_pull(task_ids='pyspark_job_submit')}}"},
    poke_interval=30,
    gcp_conn_id=GCP_CONN_ID,
    dag=dag
)

delete_dataproc_cluster = DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    project_id=GCP_PROJECT_ID,
    region=REGION,
    cluster_name=DATAPROC_CLUSTER_NAME,
    gcp_conn_id=GCP_CONN_ID,
    dag=dag
)
# [end SPARK]
