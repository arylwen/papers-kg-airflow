import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

import pendulum
from datetime import timedelta

from airflow.decorators import dag
from airflow.operators.python import get_current_context

from lib.corpus_conversion_tasks import (
    text_extractor_mapper,
    get_pdf_file_names
)

import logging
logger = logging.getLogger(__name__)

#CORPUS = 'aiml'

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
    dag_id="extract_pdf",
    default_args=DEFAULT_ARGS,
    description="DAG for converting pdf files to text for the any corpus.",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
         "CORPUS": 'docs',
     },
)

def extract_text_from_pdf():    
    # CORPUS is sent as a parameter with the DAG
    file_list = get_pdf_file_names(None)
    # virtual env tasks do not support get_current_context: https://github.com/apache/airflow/issues/34158
    text_extractor_mapper(file_list, None)

extract_text_from_pdf()
