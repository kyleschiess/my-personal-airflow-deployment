import logging
import time
import requests
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.utils.helpers import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable

from utils.core.s3.s3 import read_file_to_data, read_folder_to_data
from utils.core.scrapy.repos.parsers import parse
from utils.core.scrapy.repos.postgres import insert_to_pg

pg_hook = PostgresHook(postgres_conn_id="raw_user")
s3_hook = S3Hook(aws_conn_id="my-aws")

@dag(schedule=None, start_date=datetime(2021, 12, 1), catchup=False)
def scrapy_repos_ingest():
    scrapy_spiders = Variable.get("scrapy_spiders")
    if not scrapy_spiders:
        scrapy_spiders = "projectspider,repospider,contributorsspider,profilespider,companyspider"
    scrapy_spiders = scrapy_spiders.split(",")

    ## DEBUG ##
    #scrapy_spiders = ["companyspider"]
    ###########


    @task(task_id="check_if_scrapy_job_complete")
    def check_if_scrapy_job_complete(job_id):
        complete = False

        while not complete:
            # call scrapyd api to check if job is complete
            url = f'http://host.docker.internal:6800/listjobs.json?project=repos'
            response = requests.get(url)
            data = response.json()
            
            for job in data['running']:
                if job['id'] == job_id:
                    logging.info(f"Job {job_id} is running: {job}")

            for job in data['finished']:
                if job['id'] == job_id:
                    logging.info(f"Job {job_id} is finished: {job}")
                    complete = True

            time.sleep(10)


    @task(task_id="check_s3_bucket", retries=3, retry_delay=timedelta(seconds=10))
    def check_s3_bucket(bucket_name, key=None, prefix=None):
        if 'projectspider' in key:
            # check if file exists in s3
            found = s3_hook.check_for_key(bucket_name=bucket_name, key=key)
            if found:
                logging.info(f"Found file {key} in bucket {bucket_name}")
            else:
                raise Exception(f"Did not find file {key} in bucket {bucket_name}")
        else:
            # check if folder exists in s3
            found = s3_hook.check_for_prefix(bucket_name=bucket_name, prefix=prefix, delimiter="/")
            if found:
                logging.info(f"Found folder {prefix} in bucket {bucket_name}")
            else:
                raise Exception(f"Did not find folder {prefix} in bucket {bucket_name}")
        

    @task(task_id="s3_file_to_postgres")
    def s3_to_postgres(bucket_name, key=None, prefix=None):
        if 'projectspider' in key:
            #### get file from s3
            data = read_file_to_data(s3_hook, bucket_name, key)
        else:
            data = read_folder_to_data(s3_hook, bucket_name, prefix)

        #### parse 
        spider_name = key.split("/")[0]
        parsed_data = parse(data, spider_name)

        #### insert into postgres
        job_id = key.split("/")[1].split(".")[0]
        # add job_id to each item in parsed_data
        for item in parsed_data:
            item['job_id'] = job_id
        # insert
        insert_to_pg(hook=pg_hook, schema='raw', table=spider_name, data=parsed_data)


    @task
    def launch_spider(spider_name: str) -> str:
        # schedule job from scrapyd
        url = f'http://host.docker.internal:6800/schedule.json?project=repos&spider={spider_name}'
        response = requests.post(url)
        job_id = response.json()['jobid']
        
        return job_id
    

    @task_group(group_id="run_scrape")
    def run_scrape(spider_name: str):
        job_id = launch_spider(spider_name)

        ## DEBUG ##
        #job_id = '9a325838bebc11ee8de12aa199f11d89'
        ###########

        bucket_name = "personal-s3-bucket-1"
        prefix = f'{spider_name}/{job_id}'
        key = f'{prefix}.json.gz'
        check_if_scrapy_job_complete(job_id) >> check_s3_bucket(bucket_name=bucket_name, key=key, prefix=prefix) >> s3_to_postgres(bucket_name=bucket_name, key=key, prefix=prefix)
        

    task_groups = []
    for spider_name in scrapy_spiders:
        task_groups.append(run_scrape.override(group_id=spider_name)(spider_name))

    trigger_staging_dag = TriggerDagRunOperator(
        task_id="trigger_staging_dag",
        trigger_dag_id="scrapy_repos_staging"
    )

    chain(*task_groups + [trigger_staging_dag])

scrapy_repos_ingest()
