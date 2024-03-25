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


def upload_from_pg_to_csv(table_name: str, date: str, col_dt: str):
    logger.info(f"upload_from_pg_to_csv: table name: {table_name} date: {date} col_dt: {col_dt}")
    with psycopg2.connect(**config['postgres']) as psql_connection:
        with psql_connection.cursor() as cursor:
            query = open("/lessons/sql/03-STAGING-template-select-pg.sql").read()
            query = query.format(table_name=table_name, date=date, col_dt=col_dt)
            logger.info(f"SQL select statement: {query}")
            cursor.execute(query)
            logger.info(f"Rowcount: {cursor.rowcount}")

            rows = cursor.fetchall()
            logger.info(f"Rows number: {len(rows)}")

    tmp_dir_path = config['tmp_dir_path']
    filepath = f"{tmp_dir_path}/stg_{table_name}-{date}.csv"

    with open(filepath, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=",")
        column_names = [col[0] for col in cursor.description]
        writer.writerow(column_names)
        writer.writerows(rows)
    logger.info(f"File {filepath} was saved successfully.")


def load_from_csv_to_vertica(table_name: str, date: str):
    logger.info(f"load_from_csv_to_vertica: table name: {table_name} date: {date}")
    tmp_dir_path = config['tmp_dir_path']
    filepath = f"{tmp_dir_path}/stg_{table_name}-{date}.csv"
    logger.info(f"Input file {filepath} will be loaded to Vertica.")

    with open(filepath, 'r', newline='') as f:
        reader = csv.reader(f)
        headers = next(reader)
        column_names = (', '.join(headers))
        logger.info(f"Column names: {column_names}")

        with vertica_python.connect(**config['vertica']) as connection:
            with connection.cursor() as cursor:
                query = open("/lessons/sql/04-STAGING-template-drop-partitions.sql").read()
                query = query.format(table_name=table_name, date=date)
                logger.info(f"SQL drop-partitions statement: {query}")
                cursor.execute(query)

                query = open("/lessons/sql/05-STAGING-template-copy.sql").read()
                query = query.format(table_name=table_name, date=date, tmp_dir_path=tmp_dir_path, column_names=column_names)
                logger.info(f"SQL copy statement: {query}")
                cursor.execute(query)
                logger.info(f"Rowcount: {cursor.rowcount}")

    logger.info(f"File {filepath} was loaded to Vertica successfully.")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 1),
    "end_date": datetime(2022, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=30)
}

with DAG('stg_currencies_and_transactions', default_args=default_args, schedule_interval='@daily') as dag:
    upload_currencies_task = PythonOperator(
        task_id='extract_currencies',
        python_callable=upload_from_pg_to_csv,
        op_kwargs={"table_name": "currencies",
                   "date": "{{ ds }}",
                   "col_dt": "date_update"
                   },
        provide_context=True,
        dag=dag
    )

    stg_currencies_task = PythonOperator(
        task_id='stg_currencies',
        python_callable=load_from_csv_to_vertica,
        op_kwargs={"table_name": "currencies",
                   "date": "{{ ds }}"
                   },
        provide_context=True,
        dag=dag
    )

    upload_transactions_task = PythonOperator(
        task_id='extract_transactions',
        python_callable=upload_from_pg_to_csv,
        op_kwargs={"table_name": "transactions",
                   "date": "{{ ds }}",
                   "col_dt": "transaction_dt"
                   },
        provide_context=True,
        dag=dag
    )

    stg_transactions_task = PythonOperator(
        task_id='stg_transactions',
        python_callable=load_from_csv_to_vertica,
        op_kwargs={"table_name": "transactions",
                   "date": "{{ ds }}"
                   },
        provide_context=True,
        dag=dag
    )

    [upload_currencies_task >> stg_currencies_task]
    [upload_transactions_task >> stg_transactions_task]
