""" Setup environment Postgres

Author: Alan Uriel Camargo Cantellan
"""
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
#El PythonOperator nos permite ejecutar funciones Python dentro del workflow
from airflow.operators.python import PythonOperator
#El hoook de postgres para traernos datos
from airflow.providers.postgres.hooks.postgres import PostgresHook
#Con el operador creamos datos
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
#Agregamos operadores que nos permitan que airflow tome distintas decisiones dependiendo de la respuesta de SQL
from airflow.operators.sql import BranchSQLOperator
#Agregamos la libreria que nos permita ejecutar una acción o otra dependiendo del paso del workflow
from airflow.utils.trigger_rule import TriggerRule
#Agregamos la libreria para cargar el CSV desde el bucket
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook

def ingest_data():
    gcs_hook = GCSHook(gcp_conn_id = 'google_default')
    psql_hook = PostgresHook(postgres_conn_id='alan_conn')
    file = gcs_hook.download(object_name='user_purchase.csv', bucket_name='us-central1-de-bootcamp-786ac1aa-bucket', filename='file.csv')
    psql_hook.bulk_load(table = 'imaginary_company.user_purchase', tmp_file = file)

with DAG(
    'setup_postgres', start_date=days_ago(1), schedule_interval='@once'
    ) as dag:
    dag.doc_md = __doc__
    start_workflow = DummyOperator(task_id='start_workflow')
    validate = GCSObjectExistenceSensor(task_id='validate',
        google_cloud_conn_id = 'google_default',
        bucket = 'us-central1-de-bootcamp-786ac1aa-bucket',
        object = 'user_purchase.csv')
    prepare = PostgresOperator(task_id='prepare',
        postgres_conn_id='alan_conn',
        sql="""
            CREATE SCHEMA IF NOT EXISTS imaginary_company;
            CREATE TABLE IF NOT EXISTS imaginary_company.user_purchase (
               invoice_number varchar(10),
               stock_code varchar(20),
               detail varchar(1000),
               quantity int,
               invoice_date timestamp,
               unit_price numeric(8,3),
               customer_id int,
               country varchar(20)
            );

        """)
    clear = PostgresOperator(task_id='clear',
        postgres_conn_id='alan_conn',
        sql="""
            DELETE FROM imaginary_company.user_purchase
        """
        )
    continue_workflow = DummyOperator(task_id = 'continue_workflow')
    branch = BranchSQLOperator(
        task_id = 'is_empty',
        conn_id = 'alan_conn',
        sql = 'SELECT COUNT(*) AS rows FROM imaginary_company.user_purchase',
        follow_task_ids_if_true = [clear.task_id],
        follow_task_ids_if_false = [continue_workflow.task_id]
    )
    load = PythonOperator(task_id='load', python_callable = ingest_data, trigger_rule = TriggerRule.ONE_SUCCESS)
    end_workflow = DummyOperator(task_id='end_workflow')

    #We setup here the order of the tasks
    start_workflow >> validate >> prepare >> branch >> [clear, continue_workflow] >> load >> end_workflow
