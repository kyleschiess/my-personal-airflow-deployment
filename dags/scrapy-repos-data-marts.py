from datetime import datetime

from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    
@dag(schedule=None, start_date=datetime(2021, 12, 1), catchup=False)
def scrapy_repos_data_marts():

    create_data_marts_company_contrib_to_oss = PostgresOperator(
        task_id="create_data_marts_company_contrib_to_oss",
        postgres_conn_id="raw_user",
        sql="sql/scrapy_repos/data_marts/create_company_contrib_to_oss.sql",
    )

    trigger_metrics_dag = TriggerDagRunOperator(
        task_id="trigger_metrics_dag",
        trigger_dag_id="scrapy_repos_metrics"
    )

    create_data_marts_company_contrib_to_oss >> trigger_metrics_dag

scrapy_repos_data_marts()