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

def ingest_data():
    hook = PostgresHook(postgres_conn_id='alan_conn')
    hook.insert_rows(
        table = 'monthly_charts_data',
        rows = [
            [
                'Jan 2000',
                1,
                'The Weeknd',
                'Out Of time',
                100.01,
                1,
                2,
                3,
                4,
                5,
                6
            ]
        ]
    )

with DAG(
    'db_ingestion', start_date=days_ago(1), schedule_interval='@once'
    ) as dag:
    start_workflow = DummyOperator(task_id='start_workflow')
    validate = GCSObjectExistenceSensor(task_id='validate',
        gcp_conn_id = 'gcp_default',
        bucket_name = 'us-central1-de-bootcamp-786ac1aa-bucket',
        bucket_key = 'chart-data.csv')
    prepare = PostgresOperator(task_id='prepare',
        postgres_conn_id='alan_conn',
        sql="""
                CREATE TABLE IF NOT EXISTS monthly_charts_data (
                    month VARCHAR(10) NOT NULL,
                    position INTEGER NOT NULL,
                    artist VARCHAR(100) NOT NULL,
                    song VARCHAR(100) NOT NULL,
                    indicative_revenue NUMERIC NOT NULL,
                    us INTEGER,
                    uk INTEGER,
                    de INTEGER,
                    fr INTEGER,
                    ca INTEGER,
                    au INTEGER
                )
        """)
    clear = PostgresOperator(task_id='clear',
        postgres_conn_id='alan_conn',
        sql="""
            DELETE FROM monthly_charts_data
        """
        )
    continue_workflow = DummyOperator(task_id = 'continue_workflow')
    branch = BranchSQLOperator(
        task_id = 'is_empty',
        conn_id = 'alan_conn',
        sql = 'SELECT COUNT(*) AS rows FROM monthly_charts_data',
        follow_task_ids_if_true = [clear.task_id],
        follow_task_ids_if_false = [continue_workflow.task_id]
    )
    load = PythonOperator(task_id='load', python_callable = ingest_data, trigger_rule = TriggerRule.ONE_SUCCESS)
    end_workflow = DummyOperator(task_id='end_workflow')

    #We setup here the order of the tasks
    start_workflow >> validate >> prepare >> branch >> [clear, continue_workflow] >> load >> end_workflow
