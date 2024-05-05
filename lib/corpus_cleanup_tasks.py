import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

#import pandas as pd

from airflow.decorators import task, task_group
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from s3_utils import list_files_from_s3
from common_tasks import batch_splitter

from dataset_utils import (
    get_default_bucket,
    get_by_nc_nd,
    get_by_sa,
    get_by,
    get_nonexclusive_distrib,
    get_publicdomain,
    get_txt_raw_base,
    get_txt_cleaned_base,
)

from text_cleanup_utils import cleanup_file

import logging
logger = logging.getLogger(__name__)

@task_group
def text_cleaner_mapper(article_ids, CORPUS ):
    batches = batch_splitter(article_ids)
    papers = convert_raw_text_to_clean_text.partial(CORPUS=CORPUS).expand(files=batches)

    return papers

@task
def get_raw_txt_file_names(CORPUS, BUCKET=get_default_bucket(), params=None):
    if CORPUS is None:
        CORPUS = params['CORPUS']
        logger.info(f"corpus: {CORPUS}")
    file_list = list_files_from_s3(BUCKET, get_by_nc_nd(CORPUS, get_txt_raw_base))
    file_list.extend(list_files_from_s3(BUCKET, get_by_sa(CORPUS, get_txt_raw_base)))
    file_list.extend(list_files_from_s3(BUCKET, get_by(CORPUS, get_txt_raw_base)))
    file_list.extend(list_files_from_s3(BUCKET, get_nonexclusive_distrib(CORPUS, get_txt_raw_base)))
    file_list.extend(list_files_from_s3(BUCKET, get_publicdomain(CORPUS, get_txt_raw_base)))
    return file_list

@task(task_id="convert_raw_text_to_clean_text", max_active_tis_per_dagrun=5)
def convert_raw_text_to_clean_text(files, CORPUS, params=None):
    if CORPUS is None:
        CORPUS = params['CORPUS']
        logger.info(f"corpus: {CORPUS}")

    s3_hook = S3Hook(aws_conn_id="minio_airflow")

    def download_raw_txt_file(file):
        if not os.path.exists(file):
            logger.info(f'downloading raw txt file: {file}') 
            local_path = file.split(file.split('/')[-1])[0]
            os.makedirs(local_path, exist_ok=True)           
            file_name = s3_hook.download_file(
    		    key=file, 
    		    bucket_name=get_default_bucket(), 
    		    local_path=local_path, 
    		    preserve_file_name=False, 
            )
            logger.info(f'renaming {file_name} to {file}')
            os.rename(file_name, file)
        else:
            logger.info(f'raw txt file exists locally, skipping download: {file} ')
    
    def cleanup_raw_txt_file(raw_txt_file):
        download_raw_txt_file(raw_txt_file)
        #license folder name+filename
        short_file_name = '/'.join(file.split('/')[-2:]).split('.txt')[0]
        cleaned_txt_file_name = f'{get_txt_cleaned_base(CORPUS)}/{short_file_name}.txt'
        file_exists_in_s3 = s3_hook.check_for_key(cleaned_txt_file_name, get_default_bucket())
        if file_exists_in_s3:
            logger.info(f'txt file exists remotely, skipping conversion: {cleaned_txt_file_name}')
        else:
            os.makedirs(cleaned_txt_file_name.split(cleaned_txt_file_name.split('/')[-1])[0], exist_ok=True)
            cleanup_file(raw_txt_file, cleaned_txt_file_name)
            logger.info(f'uploading to s3: {cleaned_txt_file_name}')
            s3_hook.load_file(
    		        key=cleaned_txt_file_name, 
    		        bucket_name=get_default_bucket(), 
    		        filename=cleaned_txt_file_name, 
    		        replace=True, 
                ) 
        
    logger.info(files)

    for file in files:
        cleanup_raw_txt_file(file)