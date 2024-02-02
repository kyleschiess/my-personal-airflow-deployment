import logging, requests
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils.core.fdic_ncua.ncua import check_for_new_ncua_data
from utils.core.helpers.helpers import previous_quarter

from utils.core.fdic_ncua.fdic import check_for_new_fdic_data

pg_hook = PostgresHook(postgres_conn_id="alpharank_de_eval")

@dag(schedule="@daily", start_date=datetime(2021, 12, 1), catchup=False)
def fdic_ncua_trigger():
    today = datetime.today()
    last_quarter = previous_quarter(today)
    lq_year = str(last_quarter.year)
    lq_month = str(last_quarter.month).zfill(2)

    @task(task_id="check_for_new_fdic_data", retries=0)
    def t__check_for_new_fdic_data() -> bool:
        found_new_data = check_for_new_fdic_data(hook=pg_hook, today=today, last_quarter_dt=last_quarter)
        return found_new_data
    

    @task(task_id="check_for_new_ncua_data", retries=0)
    def t__check_for_new_ncua_data() -> bool:
        found_new_data = check_for_new_ncua_data(hook=pg_hook, last_quarter=last_quarter, lq_year=lq_year, lq_month=lq_month)
        return found_new_data
        
    trigger_fdic_ncua_ingest = TriggerDagRunOperator(
        task_id="trigger_fdic_ncua_ingest",
        trigger_dag_id="fdic_ncua_ingest",
    )
    
    @task.short_circuit()
    def check_for_new_data(cond1, cond2):
        if cond1 or cond2:
            return True
        else:
            return False
                
    new_fdic = t__check_for_new_fdic_data()
    new_ncua = t__check_for_new_ncua_data()

    found_new_data = check_for_new_data(new_fdic, new_ncua)

    trigger_fdic_ncua_ingest.set_upstream(found_new_data)

#fdic_ncua_trigger()