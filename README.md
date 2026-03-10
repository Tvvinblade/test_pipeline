# Data Pipeline: Airflow + ClickHouse + Kafka + Superset

Проект демонстрирует **end-to-end** пайплайн обработки данных от JSON-источника до аналитических дашбордов в Superset, используя Kafka для стриминга и ClickHouse как основное хранилище.

## Архитектура пайплайна

Поток данных в системе выглядит следующим образом:

```text
Источник данных (JSON)
        ↓
    Kafka Broker
        ↓
Raw Layer (ClickHouse MergeTree)
        ↓
Airflow DAGs (трансформация, обогащение)
        ↓
Analytics Layer (ClickHouse MergeTree)
        ↓
Superset (дашборды и визуализация)
