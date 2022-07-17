from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
#El hoook de postgres para traernos datos
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

with DAG(
    'db_ingestion', start_date=days_ago(1), schedule_interval='@once'
    ) as dag:
    start_workflow = DummyOperator(task_id='start_workflow')
    validate = DummyOperator(task_id='validate')
    prepare = PostgresOperator(task_id='prepare',
        postgres_conn_id='alan_conn',
        sql="""
                CREATE TABLE monthle_charts_data (
                    month VARCHAR(10) NOT NULL,
                    position INTEGER NOT NULL,
                    artist VARCHAR(100) NOT NULL,
                    song VARCHAR(100) NOT NULL,
                    indicative_resume NUMERIC NOT NULL,
                    us INTEGER,
                    uk INTEGER,
                    de INTEGER,
                    fr INTEGER,
                    ca INTEGER,
                    au INTEGER
                )
        """)
    load = DummyOperator(task_id='load')
    end_workflow = DummyOperator(task_id='end_workflow')

    #We setup here the order of the tasks
    start_workflow >> validate >> prepare >> load >> end_workflow
