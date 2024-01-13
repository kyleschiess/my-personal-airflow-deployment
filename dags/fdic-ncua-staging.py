import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

pg_hook = PostgresHook(postgres_conn_id="alpharank_de_eval")
    
@dag(schedule=None, start_date=datetime(2021, 12, 1), catchup=False)
def fdic_ncua_staging():
    
    @task(task_id="check_for_duplicates", retries=0)
    def check_for_duplicates(relation_name, dupe_col):
        dupe_sql = f"""
            WITH cte AS (
                SELECT
                    {dupe_col}
                FROM {relation_name}
            )

            SELECT
                COUNT({dupe_col}) as count,
                {dupe_col}
            FROM cte
            GROUP BY {dupe_col}
            HAVING COUNT({dupe_col}) > 1
        """

        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute(dupe_sql)
        dupes = cur.fetchall()
        cur.close()
        conn.close()

        if len(dupes) > 0:
            drop_temp_table(relation_name)
            raise Exception(f"Found {len(dupes)} duplicates")
        

    @task(task_id="drop_temp_table", retries=0)
    def drop_temp_table(relation_name):
        sql = f"""
            DROP TABLE IF EXISTS {relation_name};
        """

        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
        conn.close()
        
    #### FDIC INSTUTIONS ####
    create_staging_fdic_institutions_temp = PostgresOperator(
        task_id="create_staging_fdic_institutions_temp",
        postgres_conn_id="alpharank_de_eval",
        sql="sql/staging/create_fdic_institutions.sql",
        # add params
        params={
            "table_suffix": "_temp",
        }
    )

    create_staging_fdic_institutions_prod = PostgresOperator(
        task_id="create_staging_fdic_institutions_prod",
        postgres_conn_id="alpharank_de_eval",
        sql="sql/staging/create_fdic_institutions.sql",
        params={
            "table_suffix": "",
        }
    )

    @task_group(group_id="staging_fdic_institutions")
    def staging_fdic_institutions():
        temp_relation_name = "staging.fdic_institutions_temp"

        create_staging_fdic_institutions_temp >> check_for_duplicates(
            relation_name=temp_relation_name,
            dupe_col="dim_id",
        ) >> drop_temp_table(
            relation_name=temp_relation_name
        ) >> create_staging_fdic_institutions_prod
    

    #### FDIC FINANCIALS ####
    create_staging_fdic_financials_temp = PostgresOperator(
        task_id="create_staging_fdic_financials_temp",
        postgres_conn_id="alpharank_de_eval",
        sql="sql/staging/create_fdic_financials.sql",
        params={
            "table_suffix": "_temp",
        }
    )

    create_staging_fdic_financials_prod = PostgresOperator(
        task_id="create_staging_fdic_financials_prod",
        postgres_conn_id="alpharank_de_eval",
        sql="sql/staging/create_fdic_financials.sql",
        params={
            "table_suffix": "",
        }
    )

    @task_group(group_id="staging_fdic_financials")
    def staging_fdic_financials():
        temp_relation_name = "staging.fdic_financials_temp"

        create_staging_fdic_financials_temp >> check_for_duplicates(
            relation_name=temp_relation_name,
            dupe_col="dim_id",
        ) >> drop_temp_table(
            relation_name=temp_relation_name
        ) >> create_staging_fdic_financials_prod
    

    #### NCUA CREDIT UNION BRANCH INFORMATION ####
    create_staging_ncua_cu_branch_info_temp = PostgresOperator(
        task_id="create_staging_ncua_cu_branch_info_temp",
        postgres_conn_id="alpharank_de_eval",
        sql="sql/staging/create_ncua_cu_branch_info.sql",
        params={
            "table_suffix": "_temp",
        }
    )

    create_staging_ncua_cu_branch_info_prod = PostgresOperator(
        task_id="create_staging_ncua_cu_branch_info_prod",
        postgres_conn_id="alpharank_de_eval",
        sql="sql/staging/create_ncua_cu_branch_info.sql",
        params={
            "table_suffix": "",
        }
    )

    @task_group(group_id="staging_ncua_cu_branch_info")
    def staging_ncua_cu_branch_info():
        temp_relation_name = "staging.ncua_cu_branch_info_temp"

        create_staging_ncua_cu_branch_info_temp >> check_for_duplicates(
            relation_name=temp_relation_name,
            dupe_col="dim_id",
        ) >> drop_temp_table(
            relation_name=temp_relation_name
        ) >> create_staging_ncua_cu_branch_info_prod

    
    #### NCUA FS220 ####
    create_staging_ncua_fs220_temp = PostgresOperator(
        task_id="create_staging_ncua_fs220_temp",
        postgres_conn_id="alpharank_de_eval",
        sql="sql/staging/create_ncua_fs220.sql",
        params={
            "table_suffix": "_temp",
        }
    )

    create_staging_ncua_fs220_prod = PostgresOperator(
        task_id="create_staging_ncua_fs220_prod",
        postgres_conn_id="alpharank_de_eval",
        sql="sql/staging/create_ncua_fs220.sql",
        params={
            "table_suffix": "",
        }
    )

    @task_group(group_id="staging_ncua_fs220")
    def staging_ncua_fs220():
        temp_relation_name = "staging.ncua_fs220_temp"

        create_staging_ncua_fs220_temp >> check_for_duplicates(
            relation_name=temp_relation_name,
            dupe_col="dim_id",
        ) >> drop_temp_table(
            relation_name=temp_relation_name
        ) >> create_staging_ncua_fs220_prod

    @task_group(group_id="staging")
    def staging():
        staging_fdic_institutions()
        staging_fdic_financials()
        staging_ncua_cu_branch_info()
        staging_ncua_fs220()

    trigger_data_marts_dag = TriggerDagRunOperator(
        task_id="trigger_data_marts_dag",
        trigger_dag_id="fdic_ncua_data_marts"
    )
        
    staging() >> trigger_data_marts_dag

fdic_ncua_staging()


    