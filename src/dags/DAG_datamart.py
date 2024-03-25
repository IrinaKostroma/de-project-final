import csv
import logging
import psycopg2
import vertica_python
import yaml

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

logging.basicConfig(format="[%(asctime)s]  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

config = yaml.full_load(open("/lessons/dags/config.yaml", "r"))


def global_metrics(table_name, **context):
    date = (context['logical_date'] - timedelta(days=1)).strftime('%Y-%m-%d')

    with vertica_python.connect(**config['vertica']) as connection:
        with connection.cursor() as cursor:
            query = open("/lessons/sql/07-DWH-template-drop-partitions.sql").read()
            query = query.format(table_name=table_name, date=date)
            logger.info(f"SQL drop-partitions statement: {query}")
            cursor.execute(query)
            logger.info(f"Delete values from {table_name} by date {date}")

            query = open("/lessons/sql/08-DWH-template-insert.sql").read()
            query = query.format(table_name=table_name, date=date)
            logger.info(f"SQL insert statement: {query}")
            cursor.execute(query)
            logger.info(f"Insert values into: {table_name} from {date}")
            logger.info(f"Rowcount: {cursor.rowcount}")

            connection.commit()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 2),
    "end_date": datetime(2022, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=30)
}

with DAG('cdm_global_metrics', default_args=default_args, schedule_interval='@daily') as dag:

    global_metrics_task = PythonOperator(
        task_id='global_metrics_task',
        python_callable=global_metrics,
        op_kwargs={"table_name": "global_metrics"},
        provide_context=True,
        dag=dag
    )

    global_metrics_task
