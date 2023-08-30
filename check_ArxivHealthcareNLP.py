import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

import pendulum
from datetime import timedelta

from airflow.decorators import dag

from lib.mastodon_download_tasks import download_corpus_config

from lib.mastodon_download_tasks import download_new_toots
from lib.corpus_download_tasks import get_new_article_ids
#from lib.corpus_download_tasks import download_paper
from lib.corpus_download_tasks import cleanup


import logging
logger = logging.getLogger(__name__)

CORPUS = 'ArxivHealthcareNLP'

CORPUS_BASE = f'dags/{CORPUS}'
CORPUS_CONFIG = f'{CORPUS}.properties'
BUCKET = 'papers-kg'
PDF_BASE = f'{CORPUS_BASE}/pdf'

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
    dag_id="check_ArxivHealthcareNLP",
    default_args=DEFAULT_ARGS,
    description="DAG for uploading arxiv papers mentioned in ArxivHealthcareNLP mastodon toots.",
    schedule=None,
    catchup=False,
    max_active_runs=1
)
def check_ArxivHealthcareNLP():    
    corpus_config = download_corpus_config(CORPUS, BUCKET)
    csv_name = download_new_toots(corpus_config, CORPUS, BUCKET)
    #article_ids = get_new_article_ids(csv_name)
    #papers = download_paper.expand(paper_id=article_ids)
    # so it waits for the end - TODO find a more scalable way
    #cleanup(csv_name, papers)
    
check_ArxivHealthcareNLP()
