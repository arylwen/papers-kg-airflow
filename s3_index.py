import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

import pendulum
from datetime import timedelta

from airflow.decorators import dag

from lib.corpus_index_tasks import (
    text_indexer_mapper,
    get_cleaned_txt_file_names
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
	Indexes clean up text files into a llama_index knowledge graph.
    Uses s3 for document and index storage.
'''
@dag(
    dag_id=f"s3_index",
    default_args=DEFAULT_ARGS,
    description=f"DAG for indexing cleaned up text files for the any corpus.",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
         "CORPUS": 'docs',
     },
)
def index_clean_txt():  
    file_list = get_cleaned_txt_file_names(None)
    text_indexer_mapper(file_list, None)

index_clean_txt()
