from datetime import datetime

from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    
@dag(schedule=None, start_date=datetime(2021, 12, 1), catchup=False)
def scrapy_repos_maps():

    create_maps_company_to_contributor = PostgresOperator(
        task_id="create_maps_company_to_contributor",
        postgres_conn_id="raw_user",
        sql="sql/scrapy_repos/maps/create_company_to_contributor.sql",
    )

    trigger_data_marts_dag = TriggerDagRunOperator(
        task_id="trigger_data_marts_dag",
        trigger_dag_id="scrapy_repos_data_marts"
    )

    create_maps_company_to_contributor >> trigger_data_marts_dag

scrapy_repos_maps()