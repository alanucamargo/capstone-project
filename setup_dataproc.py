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

#Consideramos las librerias para trabajar con dataproc
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocDeleteClusterOperator

ZONE = 'us-central1-a'
REGION = 'us-central1'
CLUSTER_NAME = 'alandataproc'
PROJECT_ID = 'tribal-union-354418'

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
    create_cluster = DataprocCreateClusterOperator(task_id='create_cluster',
                    #project_id = PROJECT_ID,
                    cluster_config = CLUSTER_CONFIG,
                    region = REGION,
                    cluster_name = CLUSTER_NAME,
                    #num_workers=2,
                    #master_machine_type='n2-standard-4',
                    #worker_machine_type='n2-standard-4',
                    gcp_conn_id='google_dataproc')
    validate = DummyOperator(task_id='validate')
    prepare = DummyOperator(task_id='prepare')
    load = DummyOperator(task_id='load')
    delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster',
                    region = REGION,
                    cluster_name = CLUSTER_NAME,
                    gcp_conn_id='google_dataproc')
    end_workflow = DummyOperator(task_id='end_workflow')

    #We setup here the order of the tasks
    start_workflow >> create_cluster >> validate >> prepare >> load >> delete_cluster >> end_workflow
