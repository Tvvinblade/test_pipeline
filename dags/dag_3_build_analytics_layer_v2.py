"""
DAG 3: Построение Analytics Layer
- user_activity: объединение browser_events + device_events + geo_events + location_events
- traffic_source_metrics: utm источники с device и geo 
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from clickhouse_driver import Client

logger = logging.getLogger(__name__)

def get_clickhouse_client():
    return Client(
        host='clickhouse',
        port=9000,
        user='admin',
        password='admin',
        database='default'
    )

def build_analytics(**context):
    """Построение Analytics слоя из Raw данных"""
    client = get_clickhouse_client()
    
    
        
    # 1. user_activity
    logger.info("Building user_activity...")
    client.execute("TRUNCATE TABLE analytics.user_activity")
    
    query = """
    INSERT INTO analytics.user_activity
    SELECT
        be.click_id as user_id,
        de.device_type,
        de.os_name,
        ge.geo_country,
        ge.geo_region_name,
        COUNT(be.event_id) as total_events,
        COUNT(DISTINCT le.page_url_path) as unique_pages,
        MIN(be.event_timestamp) as first_event_time,
        MAX(be.event_timestamp) as last_event_time,
        COUNT(DISTINCT toDate(be.event_timestamp)) as session_count,
        CASE WHEN COUNT(le.event_id) > 0 THEN 1 ELSE 0 END as conversion_flag,
        now() as _updated_at
    FROM raw.browser_events be
    LEFT JOIN raw.device_events de ON be.click_id = de.click_id
    LEFT JOIN raw.geo_events ge ON be.click_id = ge.click_id
    LEFT JOIN raw.location_events le ON be.event_id = le.event_id
    GROUP BY be.click_id, de.device_type, de.os_name, ge.geo_country, ge.geo_region_name
    """
    client.execute(query)
    count = client.execute("SELECT COUNT(*) FROM analytics.user_activity")[0][0]
    
    # 2. traffic_source_metrics
    client.execute("TRUNCATE TABLE analytics.traffic_source_metrics")
    
    query = """
    INSERT INTO analytics.traffic_source_metrics
    SELECT
        le.utm_source,
        le.utm_campaign,
        le.utm_medium,
        de.device_type,
        ge.geo_country,
        COUNT(le.event_id) as visits,
        COUNT(DISTINCT be.click_id) as unique_users,
        COUNT(DISTINCT CASE WHEN le.page_url_path IS NOT NULL THEN le.event_id END) as conversions,
        ROUND(COUNT(DISTINCT CASE WHEN le.page_url_path IS NOT NULL THEN le.event_id END) * 100.0 / COUNT(DISTINCT le.event_id), 2) as conversion_rate,
        now() as _updated_at
    FROM raw.location_events le
    LEFT JOIN raw.browser_events be ON le.event_id = be.event_id
    LEFT JOIN raw.device_events de ON be.click_id = de.click_id
    LEFT JOIN raw.geo_events ge ON be.click_id = ge.click_id
    WHERE le.utm_source IS NOT NULL AND le.utm_source != ''
    GROUP BY le.utm_source, le.utm_campaign, le.utm_medium, de.device_type, ge.geo_country
    """
    client.execute(query)
    count = client.execute("SELECT COUNT(*) FROM analytics.traffic_source_metrics")[0][0]
    
    # 3. funnel_analysis
    client.execute("TRUNCATE TABLE analytics.funnel_analysis")
    
    query = """
    INSERT INTO analytics.funnel_analysis (funnel_step, visits, unique_users, step_order, _updated_at)
    SELECT
        'pageview' as funnel_step,
        COUNT(*) as visits,
        COUNT(DISTINCT click_id) as unique_users,
        1 as step_order,
        now() as _updated_at
    FROM raw.browser_events
    WHERE event_type = 'pageview'
    
    UNION ALL
    
    SELECT
        'location_visit' as funnel_step,
        COUNT(*) as visits,
        COUNT(DISTINCT be.click_id) as unique_users,
        2 as step_order,
        now() as _updated_at
    FROM raw.location_events le
    LEFT JOIN raw.browser_events be ON le.event_id = be.event_id
    WHERE le.page_url_path IS NOT NULL AND le.page_url_path != ''
    
    UNION ALL
    
    SELECT
        'device_identified' as funnel_step,
        COUNT(*) as visits,
        COUNT(DISTINCT click_id) as unique_users,
        3 as step_order,
        now() as _updated_at
    FROM raw.device_events
    WHERE device_type IS NOT NULL
    
    UNION ALL
    
    SELECT
        'geo_tagged' as funnel_step,
        COUNT(*) as visits,
        COUNT(DISTINCT click_id) as unique_users,
        4 as step_order,
        now() as _updated_at
    FROM raw.geo_events
    WHERE geo_country IS NOT NULL
    """
    client.execute(query)
    count = client.execute("SELECT COUNT(*) FROM analytics.funnel_analysis")[0][0]
        
    client.disconnect()

default_args = {
    'owner': 'data-engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'dag_3_build_analytics_layer',
    default_args=default_args,
    description='Build Analytics Layer from Raw Layer (FINAL)',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['analytics', 'etl'],
):

    task_build = PythonOperator(
        task_id='build_analytics',
        python_callable=build_analytics,
        provide_context=True,
    )

    task_build