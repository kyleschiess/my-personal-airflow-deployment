from datetime import datetime

from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    
@dag(schedule=None, start_date=datetime(2021, 12, 1), catchup=False)
def scrapy_repos_metrics():

    create_metrics_oss_repos_agg = PostgresOperator(
        task_id="create_metrics_oss_repos_agg",
        postgres_conn_id="raw_user",
        sql="sql/scrapy_repos/metrics/create_oss_repos_agg.sql",
    )

    create_metrics_oss_repos = PostgresOperator(
        task_id="create_metrics_oss_repos_w_comp",
        postgres_conn_id="raw_user",
        sql="sql/scrapy_repos/metrics/create_oss_repos_w_comp.sql",
    )

scrapy_repos_metrics()