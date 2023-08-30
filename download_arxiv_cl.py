import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

import pendulum
from datetime import timedelta

from airflow.decorators import dag

from lib.corpus_download_tasks import (
    download_pending_csv,
    get_new_article_ids,
    download_mapper,
    reconcile,
) 

import logging
logger = logging.getLogger(__name__)

CORPUS = 'arxiv_cl'

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
    dag_id="download_arxiv_cl",
    default_args=DEFAULT_ARGS,
    description="DAG for uploading arxiv papers mentioned in ArxivHealthcareNLP mastodon toots.",
    schedule=None,
    catchup=False,
    max_active_runs=1
)
def download_ArxivHealthcareNLP():    
    csv_name = download_pending_csv(CORPUS, BUCKET)
    article_ids = get_new_article_ids(csv_name)
    papers = download_mapper(article_ids, BUCKET, PDF_BASE)
    reconcile(csv_name, papers)
    
download_ArxivHealthcareNLP()
