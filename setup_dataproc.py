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


with DAG(
    'setup_postgres', start_date=days_ago(1), schedule_interval='@once'
    ) as dag:
    dag.doc_md = __doc__
    start_workflow = DummyOperator(task_id='start_workflow')
    validate = DummyOperator(task_id='validate')
    prepare = DummyOperator(task_id='prepare')
    clear = DummyOperator(task_id='clear')
    continue_workflow = DummyOperator(task_id = 'continue_workflow')
    branch = DummyOperator(task_id = 'is_empty')
    load = DummyOperator(task_id='load')
    end_workflow = DummyOperator(task_id='end_workflow')

    #We setup here the order of the tasks
    start_workflow >> validate >> prepare >> branch >> [clear, continue_workflow] >> load >> end_workflow