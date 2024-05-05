import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

import pandas as pd

from airflow.decorators import dag, task, task_group
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

#from s3_utils import list_files_from_s3
#from properties_utils import load_properties, save_properties
#from mastodon_utils import user_lookup, new_toots_to_csv
from gcloud_utils import download_paper_google
from metadata_utils import arxiv_metadata_search
from metadata_utils import dynamodb_metadata_search
from common_tasks import batch_splitter


from dataset_utils import (
    get_default_bucket,
    get_corpus_base,
)

import logging
logger = logging.getLogger(__name__)

@task_group
def download_mapper(article_ids, BUCKET, PDF_BASE):
    batches = batch_splitter(article_ids)
    papers = download_paper_batch_dynamodb.partial(BUCKET=BUCKET, PDF_BASE=PDF_BASE).expand(paper_id_list=batches)

    return papers

@task
def download_pending_csv(CORPUS, BUCKET):

    CORPUS_BASE = f'dags/{CORPUS}'
    CORPUS_BASE_CONFIG = f'dags/{CORPUS}/pending'
    PDF_BASE = f'{CORPUS_BASE}/pdf'

    if not os.path.exists(PDF_BASE):
        print(f'{PDF_BASE} does not exist. Creating.')
        os.makedirs(PDF_BASE)
    if not os.path.exists(CORPUS_BASE_CONFIG):
        print(f'{CORPUS_BASE_CONFIG} does not exist. Creating.')
        os.makedirs(CORPUS_BASE_CONFIG)

    s3_hook = S3Hook(aws_conn_id="minio_airflow")
    paths = s3_hook.list_keys(bucket_name=BUCKET, prefix=CORPUS_BASE_CONFIG)
    logger.info(f'Paths {paths}')
    csvs = [csv_name for csv_name in paths if 'csv' in csv_name]
    csv_name = csvs[0]
    logger.info(f'Downloading csv file {csv_name}')

    file_name = s3_hook.download_file(
    		key=csv_name, 
    		bucket_name=BUCKET, 
    		local_path=f'{CORPUS_BASE}/pending', 
    		preserve_file_name=True, 
    )
    logger.info(f'Downloaded csv file for {CORPUS} to {file_name}')
    return file_name

@task
def get_new_article_ids(csv_name):
    if not os.path.exists(csv_name):
	    # TODO
        print(f'{csv_name} does not exist. Downloading.')

    # TODO perhaps filter new_articles by existing in s3 so we don't trigger a task for it    
    df = pd.read_csv(csv_name)
    # drop the rows with no article id
    df.dropna(subset=['article_id'], inplace=True)
    new_articles = df.article_id.values.tolist()
    #logger.info(new_articles)
    
    return new_articles

@task(max_active_tis_per_dagrun=1)
def download_paper_arxiv(paper_id, BUCKET, PDF_BASE):
    logger.info(f'processing paper: {paper_id}')
    paper_id_str = str(paper_id)
    paper_id_str = paper_id_str.ljust(10, '0')
    logger.info(f'processing paper: {paper_id_str}')
    paper = arxiv_metadata_search(paper_id_str)
    download_paper_google(paper, BUCKET, PDF_BASE)  
    # arxiv throttling
    import time
    time.sleep(4)
    return paper_id

@task(max_active_tis_per_dagrun=50)
def download_paper_dynamodb(paper_id, BUCKET, PDF_BASE):
    logger.info(f'processing paper: {paper_id}')
    paper_id_str = str(paper_id)
    paper_id_str = paper_id_str.ljust(10, '0')
    logger.info(f'processing paper: {paper_id_str}')
    paper = dynamodb_metadata_search(paper_id_str)
    if paper:
        try:
            download_paper_google(paper, BUCKET, PDF_BASE)  
        except Exception as e:
            print(e)
            logger.info(f'*************** GOOGLE STORAGE PAPER PDF MISSING: {paper_id} *******************')
            return f'{paper_id}-missing-pdf'
    else:
        logger.info(f'*************** DYNAMODB PAPER METADATA MISSING: {paper_id} *******************')
        return f'{paper_id}-missing-metadata'
    # arxiv throttling
    #import time
    #time.sleep(4)
    return paper_id

@task(max_active_tis_per_dagrun=50)
def download_paper_batch_dynamodb(paper_id_list, BUCKET, PDF_BASE):
    result = []
    for paper_id in paper_id_list:
        logger.info(f'processing paper: {paper_id}')
        paper_id_str = str(paper_id)
        logger.info(f'searching metadata for paper: {paper_id_str}')
        paper = dynamodb_metadata_search(paper_id_str)
        if not paper:
            # did the paper lose a 0? TODO review how the csv is read
            paper_id_str = paper_id_str.ljust(10, '0')
            paper = dynamodb_metadata_search(paper_id_str)            
        if paper:
            try:
                download_paper_google(paper, BUCKET, PDF_BASE)              
                result.append(paper_id) 
            except Exception as e:
                print(e)
                logger.info(f'*************** GOOGLE STORAGE PAPER PDF MISSING: {paper_id} *******************')
                result.append(f'{paper_id}-missing-pdf')
                raise e
        else:
            logger.info(f'*************** PAPER MISSING: {paper_id} *******************')
            result.append(f'{paper_id}-missing-metadata')        
            raise Exception(f'no metadata {paper_id}')

    return result

@task
def reconcile(csv_name, papers):
    logger.info(f'Reconciling {csv_name} with uploaded papers: \n {papers}')
    has_missing = False
    all_missing = []
    # papers is a list of lists
    for paper_list in papers:
        for paper in paper_list:
            if 'missing' in str(paper):
                has_missing = True
                all_missing.append(paper)

    if has_missing:
        logger.info(f'deferring for next week: metadata or pdf missing for: {all_missing}')
    else:
        logger.info('all papers processed, moving csv {csv_name}')
        parts = csv_name.split('/') 
        corpus = parts[1] #TODO pass CORPUS as parameter
        source_csv = f'{"/".join(parts[:3])}/{parts[-1]}'
        target_csv = f'{"/".join(parts[:2])}/processed/{parts[-1]}'
        logger.info(f'{source_csv} ***** {target_csv}')
        source_conf = source_csv.split('.')[0].split(f'_{corpus}')[0]
        target_conf = target_csv.split('.')[0].split(f'_{corpus}')[0]
        logger.info(f'{source_conf} ***** {target_conf}')
        #-------
        s3_hook = S3Hook(aws_conn_id="minio_airflow")
        if not s3_hook.check_for_key(source_csv, get_default_bucket()):
            logger.info(f'csv already processed, skipping: {source_csv}')
            return
        
        if not os.path.exists(csv_name):
            csv_name = s3_hook.download_file(
    		    key=source_csv, 
    		    bucket_name=get_default_bucket(), 
    		    local_path=f'{get_corpus_base(corpus)}/pending', 
    		    preserve_file_name=True, 
            ) #TODO
        config_name = s3_hook.download_file(
    		    key=source_conf, 
    		    bucket_name=get_default_bucket(), 
    		    local_path=f'{get_corpus_base(corpus)}/pending', 
    		    preserve_file_name=True, 
            ) #TODO
        #upload csv
        s3_hook.load_file(
    		key=target_csv, 
    		bucket_name=get_default_bucket(), 
    		filename=csv_name, 
    		replace=True, 
            )
        s3_hook.delete_objects(get_default_bucket(), source_csv)
        #upload config
        s3_hook.load_file(
    		key=target_conf, 
    		bucket_name=get_default_bucket(), 
    		filename=config_name, 
    		replace=True, 
            )
        s3_hook.delete_objects(get_default_bucket(), source_conf)



@task
def cleanup(csv_name, papers, BUCKET, CORPUS, CORPUS_BASE):
    logger.info(f'---------{csv_name}')
    dir_str = os.path.dirname(csv_name)
    csv_str = csv_name.split('/')[-1]
    processed_csv_str = f'{dir_str}/processed/{csv_str}'
    s3_hook = S3Hook(aws_conn_id="minio_airflow")
    s3_hook.load_file(
    		key=processed_csv_str, 
    		bucket_name=BUCKET, 
    		filename=csv_name, 
    		replace=True, 
            )
    s3_hook.delete_objects(BUCKET, csv_name)
    logger.info(f'Moved {csv_name} to processed.')
    #load the new properties
    run_id = '_'.join(csv_str.split('_')[:-1])
    updated_properties = f'{CORPUS_BASE}/{run_id}'
    updated_processed_properties = f'{CORPUS_BASE}/processed/{run_id}'
    logger.info(f'---------{run_id} -- {updated_properties}')
    s3_hook.load_file(
    		key=updated_processed_properties, 
    		bucket_name=BUCKET, 
    		filename=updated_properties, 
    		replace=True, 
            ) 
    # finally update properties for the next run
    s3_hook.load_file(
    		key=f'{CORPUS}.properties', 
    		bucket_name=BUCKET, 
    		filename=updated_properties, 
    		replace=True, 
            ) 
    # TODO cleanup local