import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from s3_utils import list_files_from_s3
from properties_utils import load_properties, save_properties
from mastodon_utils import user_lookup, new_toots_to_csv, toots_to_csv
from gcloud_utils import download_paper_google
from metadata_utils import arxiv_metadata_search

import logging
logger = logging.getLogger(__name__)

@task
def download_corpus_config(CORPUS, BUCKET):

    CORPUS_BASE = f'dags/{CORPUS}'
    CORPUS_BASE_CONFIG = f'dags/{CORPUS}/pending'
    CORPUS_CONFIG = f'{CORPUS}.properties'
    PDF_BASE = f'{CORPUS_BASE}/pdf'

    if not os.path.exists(PDF_BASE):
        print(f'{PDF_BASE} does not exist. Creating.')
        os.makedirs(PDF_BASE)
    if not os.path.exists(CORPUS_BASE_CONFIG):
        print(f'{CORPUS_BASE_CONFIG} does not exist. Creating.')
        os.makedirs(CORPUS_BASE_CONFIG)

    s3_hook = S3Hook(aws_conn_id="minio_airflow")
    file_name = s3_hook.download_file(
    		key=CORPUS_CONFIG, 
    		bucket_name=BUCKET, 
    		local_path=f'{CORPUS_BASE}/pending', 
    		preserve_file_name=False, 
    )
    logger.info(f'Downloaded config file for {CORPUS} to {file_name}')
    return file_name

@task
def download_new_toots(corpus_file, CORPUS, BUCKET):

    CORPUS_BASE = f'dags/{CORPUS}'

    logger.info(f'Processing: {corpus_file}')
    run_id = corpus_file.split('/')[-1]
    corpus_properties = load_properties(corpus_file)
    
    ACCOUNT = corpus_properties['account']
    START_AT = int(corpus_properties['start_at']) if 'start_at' in corpus_properties else 0
    END_AT = int(corpus_properties['end_at']) if 'end_at' in corpus_properties else -1
    #LATEST = int(corpus_properties['latest']) if 'latest' in corpus_properties else -1  
    
    user = user_lookup(acct=ACCOUNT)
    user_id = user['id']
    
    logger.info(f'Downloading toots for: {ACCOUNT} start at: {START_AT} stop at: {END_AT}')
    if END_AT == -1:
        # download new toots
        csv_name, LATEST, new_toots = new_toots_to_csv(run_id, user_id, CORPUS, CORPUS_BASE, START_AT)        
    else:
        # re-process existing config
        csv_name, LATEST, new_toots = toots_to_csv(run_id, user_id, CORPUS, CORPUS_BASE, START_AT, END_AT)
    
    if new_toots > 0:
        create_new_batch(corpus_file, CORPUS, BUCKET, corpus_properties, START_AT, csv_name, LATEST) 
    else:
        logger.info(f'No new toots, skipping creating new batch for {run_id}')

    return csv_name

'''
    upload batch info to s3 for further processing
'''
def create_new_batch(corpus_file, CORPUS, BUCKET, corpus_properties, START_AT, csv_name, LATEST):

    CORPUS_BASE = f'dags/{CORPUS}'
    run_id = corpus_file.split('/')[-1]

    s3_hook = S3Hook(aws_conn_id="minio_airflow")
    s3_hook.load_file(
    		key=csv_name, 
    		bucket_name=BUCKET, 
    		filename=csv_name, 
    		replace=False, 
            )
    logger.info(f'Saved new toots to: {csv_name}')

    corpus_properties['start_at'] = START_AT
    corpus_properties['end_at'] = LATEST
    save_properties(corpus_properties, corpus_file)
    corpus_properties_key = f'{CORPUS_BASE}/pending/{run_id}' #relative to dags
    s3_hook.load_file(
    		key=corpus_properties_key, 
    		bucket_name=BUCKET, 
    		filename=corpus_file, 
    		replace=False, 
            )
    logger.info(f'Saved new run config to: s3://{corpus_properties_key}')
    # finally update properties for the next run
    corpus_properties['start_at'] = LATEST
    if 'end_at' in corpus_properties:
        del corpus_properties['end_at']
    if 'latest' in corpus_properties:
        del corpus_properties['latest']
    save_properties(corpus_properties, corpus_file)
    s3_hook.load_file(
    		key=f'{CORPUS}.properties', 
    		bucket_name=BUCKET, 
    		filename=corpus_file, 
    		replace=True, 
            )