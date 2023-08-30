import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

import pendulum
from datetime import timedelta

from airflow.decorators import dag

from lib.corpus_cleanup_tasks import (
    text_cleaner_mapper,
    get_raw_txt_file_names
)

import logging
logger = logging.getLogger(__name__)

CORPUS = 'arxiv_cl'

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2023, 1, 18, tz="UTC"),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

'''
	Checks the mastodon bot account for new papers posted.
	Download pdf files and upload them to S3
'''
@dag(
    dag_id="raw_text_to_clean_text_aarxiv_cl",
    default_args=DEFAULT_ARGS,
    description="DAG for converting cleaning raw txt files for the arxiv_cl corpus.",
    schedule=None,
    catchup=False,
    max_active_runs=1
)
def cleanup_raw_txt():    
    file_list = get_raw_txt_file_names(CORPUS)
    text_cleaner_mapper(file_list, CORPUS)

cleanup_raw_txt()
