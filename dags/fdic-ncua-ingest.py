import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils.core.helpers.postgres import insert_to_pg
from utils.core.helpers.helpers import previous_quarter
from utils.core.ncua.ncua import check_for_new_ncua_data, get_ncua_call_report_file, ncua_call_report_to_s3
from utils.core.fdic.fdic import check_for_new_fdic_data, get_fdic_data

pg_hook = PostgresHook(postgres_conn_id="alpharank_de_eval")
s3_hook = S3Hook(aws_conn_id="my-aws")

@dag(schedule="@daily", start_date=datetime(2021, 12, 1), catchup=False)
def fdic_ncua_ingest():
    
    today = datetime.today()

    ### DEBUG
    #today = datetime(2023, 6, 29)
    ###

    last_quarter = previous_quarter(today)
    lq_year = str(last_quarter.year)
    lq_month = str(last_quarter.month).zfill(2)

    print(f"Today: {today}")
    print(f"Last Quarter: {last_quarter}")

    @task(task_id="extract_fdic_institutions", retries=0)
    def extract_fdic_institutions():
        # insert into postgres
        insert_to_pg(hook=pg_hook, schema='raw', table='fdic_institutions', data=get_fdic_data("/institutions", today_dt=today, last_quarter_dt=last_quarter))


    @task(task_id="extract_fdic_financials", retries=0)
    def extract_fdic_financials():
        # insert into postgres
        insert_to_pg(hook=pg_hook, schema='raw', table='fdic_financials', data=get_fdic_data("/financials", today_dt=today, last_quarter_dt=last_quarter))


    @task(task_id="extract_ncua_call_report_data", retries=0)
    def extract_ncua_call_report_data():
        ncua_call_report_to_s3(s3_hook, lq_year, lq_month)

    @task(task_id="s3_check_for_ncua_call_report_data", retries=3, retry_delay=timedelta(seconds=10))
    def s3_check_for_ncua_call_report_data():
        found = s3_hook.check_for_key(bucket_name="alpharank-de-eval", key=f"ncua-call-report-data/{lq_year}-{lq_month}.zip")
        if found:
            logging.info(f"Found ncua call report data for {lq_year}-{lq_month}")
        else:
            raise Exception(f"Did not find ncua call report data for {lq_year}-{lq_month}")
        

    @task(task_id="extract_ncua_credit_union_branch_information", retries=0)
    def extract_ncua_credit_union_branch_information():
        file_name = 'Credit Union Branch Information.txt'

        insert_to_pg(
            hook=pg_hook, 
            schema='raw', 
            table='ncua_credit_union_branch_information', 
            data=get_ncua_call_report_file(
                hook=s3_hook, 
                year=lq_year, 
                quarter=lq_month, 
                file_name=file_name
            )
        )


    @task(task_id="extract_ncua_acct_desc", retries=0)
    def extract_ncua_acct_desc():
        file_name = 'AcctDesc.txt'

        insert_to_pg(
            hook=pg_hook, 
            schema='raw', 
            table='ncua_acct_desc', 
            data=get_ncua_call_report_file(
                hook=s3_hook, 
                year=lq_year, 
                quarter=lq_month, 
                file_name=file_name
            )
        )


    @task(task_id="extract_ncua_fs220", retries=0)
    def extract_ncua_fs220():
        file_name = 'FS220.txt'

        insert_to_pg(
            hook=pg_hook, 
            schema='raw', 
            table='ncua_fs220', 
            data=get_ncua_call_report_file(
                hook=s3_hook, 
                year=lq_year, 
                quarter=lq_month, 
                file_name=file_name
            )
        )

    
    @task_group(group_id="fdic")
    def fdic():
        extract_fdic_institutions()
        extract_fdic_financials()


    @task_group(group_id="ncua")
    def ncua():
        extract_ncua_call_report_data() >> s3_check_for_ncua_call_report_data() >> [
            extract_ncua_credit_union_branch_information(),
            extract_ncua_acct_desc(),
            extract_ncua_fs220(),
        ]


    trigger_staging_dag = TriggerDagRunOperator(
        task_id="trigger_staging_dag",
        trigger_dag_id="fdic_ncua_staging"
    )


    @task.short_circuit()
    def t__check_for_new_fdic_data() -> bool:
        found_new_data = check_for_new_fdic_data(hook=pg_hook, today=today, last_quarter=last_quarter)
        return found_new_data
    

    @task.short_circuit()
    def t__check_for_new_ncua_data() -> bool:
        found_new_data = check_for_new_ncua_data(hook=pg_hook, last_quarter=last_quarter, lq_year=lq_year, lq_month=lq_month)
        return found_new_data
                
        
    t__check_for_new_fdic_data() >> fdic() >> trigger_staging_dag
    t__check_for_new_ncua_data() >> ncua() >> trigger_staging_dag

    
fdic_ncua_ingest()