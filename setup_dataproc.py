""" Setup environment Dataproc

Author: Alan Uriel Camargo Cantellan
"""
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
#El PythonOperator nos permite ejecutar funciones Python dentro del workflow
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
#Agregamos la libreria que nos permita ejecutar una acción o otra dependiendo del paso del workflow
from airflow.utils.trigger_rule import TriggerRule
#Agregamos la libreria para cargar el CSV desde el bucket
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook

#Consideramos las librerias para trabajar con el capstone-project
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocDeleteClusterOperator, DataprocSubmitJobOperator, DataprocSubmitPySparkJobOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

#Librerias para manejar BigQuery
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateExternalTableOperator, BigQueryCreateEmptyTableOperator

GCS_BUCKET = 'us-central1-de-bootcamp-786ac1aa-bucket'
GCS_OBJECT_PATH = 'data'
SOURCE_TABLE_NAME = 'imaginary_company.user_purchase'
POSTGRES_CONNECTION_ID = 'alan_conn'

ZONE = 'us-central1-a'
REGION = 'us-central1'
CLUSTER_NAME = 'alandataproc'
PROJECT_ID = 'tribal-union-354418'
PYSPARK_JOB = ''
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": "gs://us-central1-de-bootcamp-786ac1aa-bucket/scripts/script.py"},
}
GOOGLE_CONN_ID = 'google_dataproc'
GOOGLE_CONN_BIGQUERY_ID = 'google_bigquery'
DATASET_NAME = 'DW'

CLUSTER_CONFIG = {
  "config_bucket": "us-central1-de-bootcamp-786ac1aa-bucket",
  "temp_bucket": "us-central1-de-bootcamp-786ac1aa-bucket",
  "gce_cluster_config": {
      "zone_uri": ZONE,
      "network_uri": "default"
  },
  "master_config": {
      "num_instances": 1,
      "machine_type_uri": "n1-standard-2",
      "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024}
  },
  "worker_config": {
      "num_instances": 2,
      "machine_type_uri": "n1-standard-2",
      "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024}
  },
}

with DAG(
    'setup_dataproc', start_date=days_ago(1), schedule_interval='@once'
    ) as dag:
    dag.doc_md = __doc__
    start_workflow = DummyOperator(task_id='start_workflow')
    """
    validate_object = DummyOperator(task_id='validate_object')
    eliminate_object = DummyOperator(task_id='eliminate_object')
    continue_to_create_object = DummyOperator(task_id='continue_to_create_object')
    postgres_to_gcs_task = PostgresToGCSOperator(
                    task_id='postgres_to_gcs',
                    postgres_conn_id=POSTGRES_CONNECTION_ID,
                    sql=f'SELECT * FROM {SOURCE_TABLE_NAME};',
                    bucket=GCS_BUCKET,
                    filename=f'{GCS_OBJECT_PATH}/user_purchase_data.csv',
                    export_format='csv',
                    gzip=False,
                    use_server_side_cursor=False)
    create_cluster = DataprocCreateClusterOperator(
                    task_id='create_cluster',
                    cluster_config = CLUSTER_CONFIG,
                    region = REGION,
                    cluster_name = CLUSTER_NAME,
                    gcp_conn_id=GOOGLE_CONN_ID)
    pyspark_task = DataprocSubmitJobOperator(
                    task_id='pyspark_task',
                    job=PYSPARK_JOB,
                    region=REGION,
                    project_id=PROJECT_ID,
                    gcp_conn_id=GOOGLE_CONN_ID)
    delete_cluster = DataprocDeleteClusterOperator(
                    task_id='delete_cluster',
                    region = REGION,
                    cluster_name = CLUSTER_NAME,
                    gcp_conn_id=GOOGLE_CONN_ID,
                    trigger_rule = TriggerRule.ALL_DONE)
                    """
    create_dataset = BigQueryCreateEmptyDatasetOperator(
                    task_id="create_dataset",
                    dataset_id=DATASET_NAME,
                    gcp_conn_id=GOOGLE_CONN_BIGQUERY_ID,
                    exists_ok=True)
    review_logs_external_table = BigQueryCreateExternalTableOperator(
                    task_id="review_logs_external_table",
                    bucket=GCS_BUCKET,
                    destination_project_dataset_table=f"{DATASET_NAME}.review_logs",
                    source_objects=['gs://us-central1-de-bootcamp-786ac1aa-bucket/stage/review_logs.parquet'],
                    google_cloud_storage_conn_id='google_default')
    end_workflow = DummyOperator(
                    task_id='end_workflow')

    #We setup here the order of the tasks
    #start_workflow >> validate_object >> [eliminate_object, continue_to_create_object] >> postgres_to_gcs_task >> create_cluster >> pyspark_task >> delete_cluster >> end_workflow
    start_workflow >> create_dataset >> review_logs_external_table >> end_workflow
