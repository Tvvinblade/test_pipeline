"""
DAG "load_jsonl_to_kafka"
- Полученные jsonl отправляем в kafka
- создаем отдельный топик для каждого файла
"""

from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import json
import os
from kafka import KafkaProducer
import time
import logging

logger = logging.getLogger(__name__)

# Папка с файлами (ВНУТРИ контейнера)
DATA_DIR = "/opt/data"

# Kafka настройки (Docker Compose сеть)
KAFKA_BROKER = "kafka:29093"

TOPICS = {
    "browser_events.jsonl": "browser_events",
    "device_events.jsonl": "device_events",
    "geo_events.jsonl": "geo_events",
    "location_events.jsonl": "location_events"
}

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='send_jsonl_to_kafka',
    default_args=default_args,
    description='DAG для отправки JSONL файлов в Kafka',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['kafka', 'jsonl'],
) as dag:

    @task
    def send_file_to_kafka(filename: str, topic: str):
        filepath = os.path.join(DATA_DIR, filename)
        
        if not os.path.exists(filepath):
            logger.error(f"Файл не найден: {filepath}")
            raise FileNotFoundError(f"File not found: {filepath}")
        
        logger.info(f"Подключение к Kafka: {KAFKA_BROKER}")
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Ждать подтверждения от всех replicas
            retries=3,
            max_in_flight_requests_per_connection=1
        )

        logger.info(f"Отправка данных из {filename} в топик {topic}")
        count = 0
        errors = 0
        
        try:
            with open(filepath, "r") as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        event = json.loads(line.strip())
                        future = producer.send(topic, value=event)
                        # Ждем подтверждения отправки
                        record_metadata = future.get(timeout=10)
                        count += 1
                        
                        if line_num % 100 == 0:
                            logger.info(f"  Отправлено {count} сообщений...")
                            
                    except json.JSONDecodeError as e:
                        logger.warning(f"Ошибка JSON в строке {line_num}: {str(e)[:50]}")
                        errors += 1
                    except Exception as e:
                        logger.error(f"Ошибка отправки в строке {line_num}: {str(e)}")
                        errors += 1
        finally:
            # Обязательно закрываем producer
            producer.flush()
            producer.close()
        
        logger.info(f"{filename}: отправлено {count} сообщений в топик {topic} (ошибок: {errors})")
        return f"Successfully sent {count} messages from {filename} to {topic}"
    
    dag.doc_md = __doc__

    # Создаем задачи для каждого файла
    tasks = [send_file_to_kafka(filename, topic) for filename, topic in TOPICS.items()]