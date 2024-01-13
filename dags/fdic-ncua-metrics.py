import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.operators.postgres import PostgresOperator

@dag(schedule=None, start_date=datetime(2021, 12, 1), catchup=False)
def fdic_ncua_metrics():
    
    #### BANK CU ASSET DEP ####
    create_metrics_bank_cu_asset_dep = PostgresOperator(
        task_id="create_metrics_bank_cu_asset_dep",
        postgres_conn_id="alpharank_de_eval",
        sql="sql/metrics/create_bank_cu_asset_dep.sql",
    )
    
fdic_ncua_metrics()