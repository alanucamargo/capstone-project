from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

with DAG('db_ingestion', start_date=days_ago) as dag:
    start_workflow = DummyOperator(task_id='start_workflow')
    validate = DummyOperator(task_id='validate')
    prepare = DummyOperator(task_id='prepare')
    load = DummyOperator(task_id='load')
    end_workflow = DummyOperator(task_id='end_workflow')

    #We setup here the order of the tasks
    start_workflow >> validate >> prepare >> load >> end_workflow
