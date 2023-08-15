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

def sync_blob_to_local(**kwargs):
    files_list = kwargs['ti'].xcom_pull(task_ids='list_raw_sinan_leivis')
    local_data_path = '/usr/local/airflow/dags/data'

    if not os.path.exists(os.path.join(local_data_path, 'raw/sinan_leivis')):
        os.makedirs(os.path.join(local_data_path, 'raw/sinan_leivis'))
    if not os.path.exists(os.path.join(local_data_path, 'processing/sinan_leivis')):
        os.makedirs(os.path.join(local_data_path, 'processing/sinan_leivis'))

    for input_blob in files_list[1:]:
        GCSHook(
            gcp_conn_id=GCP_CONN_ID
        ).download(
            bucket_name=RAW_BUCKET,
            object_name=input_blob,
            filename=f'{local_data_path}/raw/{input_blob}'
        )
    return os.listdir(os.path.join(local_data_path, 'raw', 'sinan_leivis'))


def csv_to_parquet():
    local_data_path = '/usr/local/airflow/dags/data'
    files = os.listdir(os.path.join(local_data_path, 'processing', 'sinan_leivis'))
    data = pd.DataFrame()

    for it in files:
        data = pd.concat([
            data,
            pd.read_csv(
                os.path.join(local_data_path, 'processing', 'sinan_leivis', it),
                encoding='latin1',
                low_memory=False)
        ], axis=0)

    to_float = ['TP_NOT', 'SG_UF_NOT', 'CS_GESTANT', 'CS_RACA', 'CS_ESCOL_N', 'SG_UF', 'ID_PAIS', 'NDUPLIC_N',
                'CS_FLXRET', 'FLXRECEBI', 'MIGRADO_W', 'FEBRE', 'FRAQUEZA', 'EDEMA', 'EMAGRA',
                'TOSSE', 'PALIDEZ', 'BACO', 'INFECCIOSO', 'FEN_HEMORR', 'FIGADO', 'ICTERICIA', 'OUTROS', 'HIV',
                'DIAG_PAR_N', 'IFI', 'OUTRO', 'ENTRADA', 'DROGA', 'FALENCIA', 'CLASSI_FIN', 'CRITERIO', 'TPAUTOCTO',
                'COPAISINF', 'DOENCA_TRA', 'EVOLUCAO', 'CO_PAIS_1', 'CO_PAIS_2', 'CO_PAIS_3', 'PESO', 'DOSE', 'AMPOLAS'
                ]
    to_str = ['ID_AGRAVO', 'SEM_NOT', 'NU_ANO', 'ID_MUNICIP', 'ID_REGIONA', 'SEM_PRI', 'NU_IDADE_N', 'CS_SEXO',
              'ID_MN_RESI', 'ID_RG_RESI', 'ID_OCUPA_N', 'OUTROS_ESP', 'COUFINF', 'COMUNINF', 'DS_MUN_1', 'DS_MUN_2',
              'DS_MUN_3', 'CO_UF_1', 'CO_UF_2', 'CO_UF_3', 'DS_TRANS_1', 'DS_TRANS_2', 'DS_TRANS_3',
              ]
    to_date = ['DT_NOTIFIC', 'DT_SIN_PRI', 'DT_NASC', 'DT_INVEST', 'TRATAMENTO', 'DT_OBITO', 'DT_ENCERRA', 'DT_DESLC1',
               'DT_DESLC2', 'DT_DESLC3']

    data[to_float] = data[to_float].astype(float)
    data[to_str] = data[to_str].astype(str)
    data[to_date] = data[to_date].astype(str)
    data = data[to_float + to_str + to_date]
    data.to_parquet(os.path.join(local_data_path, 'processing', 'sinan_leivis', '2007_2020_leivis.parquet'))
    return True


def get_parquet_schema():
    import pyarrow.parquet as pq
    import json
    local_data_path = '/usr/local/airflow/dags/data'
    p = pq.read_table(os.path.join(local_data_path, 'processing', 'sinan_leivis', '2007_2020_leivis.parquet'))
    schema = p.schema
    schema = [dict(zip(["name", "type"], it)) for it in [it.split(": ") for it in str(schema).split('\n')][:75]]
    _ = [it.update({'mode': 'NULLABLE'}) for it in schema]
    schema = json.dumps(schema)
    return schema


def sync_local_to_blob(**kwargs):
    local_data_path = '/usr/local/airflow/dags/data'
    file = os.listdir(os.path.join(local_data_path, 'processing', 'sinan_leivis'))
    file = [it for it in file if '.parquet' in it]

    GCSHook(
        gcp_conn_id=GCP_CONN_ID
    ).upload(
        bucket_name=PROCESSING_BUCKET,
        object_name=f'sinan_leivis/{file[0]}',
        filename=f'{local_data_path}/processing/sinan_leivis/{file[0]}'
    )
    return True


def delete_local_data():
    def silent_remove(file):
        import errno
        try:
            os.remove(file)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

    local_data_path = '/usr/local/airflow/dags/data'
    for it in os.listdir(os.path.join(local_data_path, 'raw/sinan_leivis')):
        silent_remove(os.path.join(local_data_path, 'raw/sinan_leivis', it))
    for it in os.listdir(os.path.join(local_data_path, 'processing/sinan_leivis')):
        silent_remove(os.path.join(local_data_path, 'processing/sinan_leivis', it))


# [START dag]
dag = DAG(
    dag_id='sinan_leivis_raw_pipeline',
    default_args=default_args,
    start_date=datetime(year=2023, month=7, day=31),
    schedule_interval='@daily',
    catchup=False,
    tags=['RAW', 'BUCKET_TO_BQ']
)
# [END dag]

# [START set_tasks]
start = EmptyOperator(task_id='start')

list_raw_sinan_leivis = GCSListObjectsOperator(
    task_id='list_raw_sinan_leivis',
    bucket=RAW_BUCKET,
    prefix='sinan_leivis',
    gcp_conn_id=GCP_CONN_ID,
    dag=dag
)

sync_raw_sinan_leivis_to_local = PythonOperator(
    task_id='sync_raw_sinan_leivis_to_local',
    python_callable=sync_blob_to_local,
    dag=dag
)

convert_dbc_to_csv = BashOperator(
    task_id='convert_dbc_to_csv',
    bash_command='Rscript /usr/local/airflow/dags/app/spatiotemporal_processing/dbc_to_csv.r /usr/local/airflow/dags/data/raw/sinan_leivis /usr/local/airflow/dags/data/processing/sinan_leivis',
    dag=dag
)

convert_csv_to_parquet = PythonOperator(
    task_id='convert_csv_to_parquet',
    python_callable=csv_to_parquet,
    dag=dag
)

create_processing_bucket = GCSCreateBucketOperator(
    task_id='create_processing_bucket',
    project_id=GCP_PROJECT_ID,
    bucket_name=PROCESSING_BUCKET,
    storage_class='REGIONAL',
    location=LOCATION,
    labels={'env': 'dev', 'team': 'airflow'},
    gcp_conn_id=GCP_CONN_ID,
    dag=dag
)

sync_local_sinan_leivis_to_processing = PythonOperator(
    task_id='sync_local_sinan_leivis_to_processing',
    python_callable=sync_local_to_blob,
    dag=dag
)

list_processing_sinan_leivis = GCSListObjectsOperator(
    task_id='list_processing_sinan_leivis',
    bucket=PROCESSING_BUCKET,
    prefix='sinan_leivis',
    gcp_conn_id=GCP_CONN_ID,
    dag=dag
)

delete_local_sinan_leivis = PythonOperator(
    task_id='delete_local_sinan_leivis',
    python_callable=delete_local_data,
    dag=dag
)

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
    schema_fields=get_parquet_schema,
    location=LOCATION,
    gcp_conn_id=GCP_CONN_ID,
    dag=dag
)

ingest_processing_bucket_into_bq_table = GCSToBigQueryOperator(
    task_id='ingest_processing_bucket_into_bq_table',
    bucket=PROCESSING_BUCKET,
    source_objects=['sinan_leivis/*.parquet'],
    destination_project_dataset_table=f'{GCP_PROJECT_ID}:{BQ_DATASET_NAME}.{BQ_SINAN_TABLE_NAME}',
    source_format='parquet',
    create_disposition='CREATE_NEVER',
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    autodetect=True,
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

delete_processing_bucket = GCSDeleteBucketOperator(
    task_id='delete_processing_bucket',
    bucket_name=PROCESSING_BUCKET,
    gcp_conn_id=GCP_CONN_ID,
    dag=dag
)

(start >>
 list_raw_sinan_leivis >>
 sync_raw_sinan_leivis_to_local >>
 convert_dbc_to_csv >>
 convert_csv_to_parquet >>
 create_processing_bucket >>
 sync_local_sinan_leivis_to_processing >>
 list_processing_sinan_leivis >>
 bq_create_dataset >>
 bq_create_table >>
 ingest_processing_bucket_into_bq_table >>
 check_bq_table >>
 delete_local_sinan_leivis >>
 delete_processing_bucket
 )
