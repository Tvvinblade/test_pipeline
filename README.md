Data Pipeline: Airflow + ClickHouse + Kafka + Superset

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


