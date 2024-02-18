from datetime import datetime

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

pg_hook = PostgresHook(postgres_conn_id="raw_user")
    
@dag(schedule=None, start_date=datetime(2021, 12, 1), catchup=False)
def scrapy_repos_staging():
    
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

    #### Project Spider ####
    create_staging_projectspider_temp = PostgresOperator(
        task_id="create_staging_projectspider_temp",
        postgres_conn_id="raw_user",
        sql="sql/scrapy_repos/staging/create_projectspider.sql",
        # add params
        params={
            "table_suffix": "_temp",
        }
    )

    create_staging_projectspider_prod = PostgresOperator(
        task_id="create_staging_projectspider_prod",
        postgres_conn_id="raw_user",
        sql="sql/scrapy_repos/staging/create_projectspider.sql",
        # add params
        params={
            "table_suffix": "",
        }
    )

    @task_group(group_id="staging_projectspider")
    def staging_projectspider():
        temp_relation_name = "staging.projectspider_temp"

        create_staging_projectspider_temp >> check_for_duplicates(
            relation_name=temp_relation_name,
            dupe_col="dim_id",
        ) >> drop_temp_table(
            relation_name=temp_relation_name
        ) >> create_staging_projectspider_prod

    
    #### Repo Spider ####
    create_staging_repospider_temp = PostgresOperator(
        task_id="create_staging_repospider_temp",
        postgres_conn_id="raw_user",
        sql="sql/scrapy_repos/staging/create_repospider.sql",
        # add params
        params={
            "table_suffix": "_temp",
        }
    )

    create_staging_repospider_prod = PostgresOperator(
        task_id="create_staging_repospider_prod",
        postgres_conn_id="raw_user",
        sql="sql/scrapy_repos/staging/create_repospider.sql",
        # add params
        params={
            "table_suffix": "",
        }
    )

    @task_group(group_id="staging_repospider")
    def staging_repospider():
        temp_relation_name = "staging.repospider_temp"

        create_staging_repospider_temp >> check_for_duplicates(
            relation_name=temp_relation_name,
            dupe_col="dim_id",
        ) >> drop_temp_table(
            relation_name=temp_relation_name
        ) >> create_staging_repospider_prod


    #### Contributors Spider ####
    create_staging_contributorsspider_temp = PostgresOperator(
        task_id="create_staging_contributorsspider_temp",
        postgres_conn_id="raw_user",
        sql="sql/scrapy_repos/staging/create_contributorsspider.sql",
        # add params
        params={
            "table_suffix": "_temp",
        }
    )

    create_staging_contributorsspider_prod = PostgresOperator(
        task_id="create_staging_contributorsspider_prod",
        postgres_conn_id="raw_user",
        sql="sql/scrapy_repos/staging/create_contributorsspider.sql",
        # add params
        params={
            "table_suffix": "",
        }
    )

    @task_group(group_id="staging_contributorsspiderr")
    def staging_contributorsspider():
        temp_relation_name = "staging.contributorsspider_temp"

        create_staging_contributorsspider_temp >> check_for_duplicates(
            relation_name=temp_relation_name,
            dupe_col="dim_id",
        ) >> drop_temp_table(
            relation_name=temp_relation_name
        ) >> create_staging_contributorsspider_prod


    #### Profile Spider ####
    create_staging_profilespider_temp = PostgresOperator(
        task_id="create_staging_profilespider_temp",
        postgres_conn_id="raw_user",
        sql="sql/scrapy_repos/staging/create_profilespider.sql",
        # add params
        params={
            "table_suffix": "_temp",
        }
    )

    create_staging_profilespider_prod = PostgresOperator(
        task_id="create_staging_profilespider_prod",
        postgres_conn_id="raw_user",
        sql="sql/scrapy_repos/staging/create_profilespider.sql",
        # add params
        params={
            "table_suffix": "",
        }
    )

    @task_group(group_id="staging_profilespiderr")
    def staging_profilespider():
        temp_relation_name = "staging.profilespider_temp"

        create_staging_profilespider_temp >> check_for_duplicates(
            relation_name=temp_relation_name,
            dupe_col="dim_id",
        ) >> drop_temp_table(
            relation_name=temp_relation_name
        ) >> create_staging_profilespider_prod


    #### Company Spider ####
    create_staging_companyspider_temp = PostgresOperator(
        task_id="create_staging_companyspider_temp",
        postgres_conn_id="raw_user",
        sql="sql/scrapy_repos/staging/create_companyspider.sql",
        # add params
        params={
            "table_suffix": "_temp",
        }
    )

    create_staging_companyspider_prod = PostgresOperator(
        task_id="create_staging_companyspider_prod",
        postgres_conn_id="raw_user",
        sql="sql/scrapy_repos/staging/create_companyspider.sql",
        # add params
        params={
            "table_suffix": "",
        }
    )

    @task_group(group_id="staging_companyspiderr")
    def staging_companyspider():
        temp_relation_name = "staging.companyspider_temp"

        create_staging_companyspider_temp >> check_for_duplicates(
            relation_name=temp_relation_name,
            dupe_col="dim_id",
        ) >> drop_temp_table(
            relation_name=temp_relation_name
        ) >> create_staging_companyspider_prod


    @task_group(group_id="staging")
    def staging():
        staging_projectspider()
        staging_repospider()
        staging_contributorsspider()
        staging_profilespider()
        staging_companyspider()

    trigger_maps_dag = TriggerDagRunOperator(
        task_id="trigger_maps_dag",
        trigger_dag_id="scrapy_repos_maps"
    )
        
    staging() >> trigger_maps_dag

scrapy_repos_staging()