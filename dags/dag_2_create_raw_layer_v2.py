"""
DAG 2: Создание Raw Layer в ClickHouse
- Создаёт базу данных raw
- Создаёт Raw MergeTree таблицы (БЕЗ Kafka Engine)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def get_clickhouse_client():
    return Client(
        host='clickhouse',
        port=9000,
        user='admin',
        password='admin',
        database='default'
    )

def load_sql_file(filename: str) -> str:
    sql_path = Path('/opt/sql') / filename
    logger.info(f"Loading SQL from: {sql_path}")
    with open(sql_path, 'r', encoding='utf-8') as f:
        return f.read()

def create_database(**context):
    client = get_clickhouse_client()
    try:
        query = "CREATE DATABASE IF NOT EXISTS raw;"
        client.execute(query)
        logger.info("✅ Database 'raw' created successfully")
    except Exception as e:
        logger.error(f"❌ Error creating database: {e}")
        raise
    finally:
        client.disconnect()

def create_raw_tables(**context):
    client = get_clickhouse_client()
    try:
        sql = load_sql_file('create_raw_tables.sql')
        queries = [q.strip() for q in sql.split(';') if q.strip()]
        for query in queries:
            logger.info(f"Executing: {query[:80]}...")
            client.execute(query)
        logger.info(f"✅ Created {len(queries)} Raw MergeTree tables")
    except Exception as e:
        logger.error(f"❌ Error creating raw tables: {e}")
        raise
    finally:
        client.disconnect()

default_args = {
    'owner': 'data-engineer',
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    'dag_2_create_raw_layer',
    default_args=default_args,
    description='Create Raw Layer in ClickHouse (MergeTree only)',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['clickhouse', 'raw-layer', 'etl'],
):

    task_create_database = PythonOperator(
        task_id='create_database',
        python_callable=create_database,
        provide_context=True,
    )

    task_create_raw = PythonOperator(
        task_id='create_raw_tables',
        python_callable=create_raw_tables,
        provide_context=True,
    )

    task_create_database >> task_create_raw