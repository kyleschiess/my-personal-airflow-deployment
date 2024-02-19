import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(schedule=None, start_date=datetime(2021, 12, 1), catchup=False)
def fdic_ncua_data_marts():
    
    #### FDIC BANKS ####
    create_data_marts_fdic_banks= PostgresOperator(
        task_id="create_data_marts_fdic_banks",
        postgres_conn_id="raw_user",
        sql="sql/fdic_ncua/data_marts/create_fdic_banks.sql",
    )

    #### NCUA CREDIT UNIONS ####
    create_data_marts_ncua_credit_unions = PostgresOperator(
        task_id="create_data_marts_ncua_credit_unions",
        postgres_conn_id="raw_user",
        sql="sql/fdic_ncua/data_marts/create_ncua_credit_unions.sql",
    )

    @task_group(group_id="create_data_marts")
    def create_data_marts():
        create_data_marts_fdic_banks
        create_data_marts_ncua_credit_unions

    trigger_metrics_dag = TriggerDagRunOperator(
        task_id="trigger_metrics_dag",
        trigger_dag_id="fdic_ncua_metrics"
    )


    create_data_marts() >> trigger_metrics_dag

fdic_ncua_data_marts()