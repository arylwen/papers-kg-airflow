import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

import pandas as pd

from airflow.decorators import task, task_group

from s3_utils import list_files_from_s3
from common_tasks import batch_splitter

from dataset_utils import (
    get_default_bucket,
    get_by_nc_nd,
    get_by_sa,
    get_by,
    get_nonexclusive_distrib,
    get_publicdomain
)

import logging
logger = logging.getLogger(__name__)

@task_group
def text_extractor_mapper(article_ids, CORPUS ):
    batches = batch_splitter(article_ids)
    papers = convert_pdf_to_text.partial(CORPUS=CORPUS).expand(files=batches)

    return papers

@task
def get_pdf_file_names(CORPUS, BUCKET=get_default_bucket(), params=None):
    if CORPUS is None:
        CORPUS = params['CORPUS']
        logger.info(f"corpus: {CORPUS}")
    file_list = list_files_from_s3(BUCKET, get_by_nc_nd(CORPUS))
    file_list.extend(list_files_from_s3(BUCKET, get_by_sa(CORPUS)))
    file_list.extend(list_files_from_s3(BUCKET, get_by(CORPUS)))
    file_list.extend(list_files_from_s3(BUCKET, get_nonexclusive_distrib(CORPUS)))
    file_list.extend(list_files_from_s3(BUCKET, get_publicdomain(CORPUS)))
    return file_list

@task.virtualenv(
    task_id="extract_pdf", requirements=["pymupdf>=1.22.5"], system_site_packages=True, max_active_tis_per_dagrun=5
)
def convert_pdf_to_text(files, CORPUS, params=None):
    import os
    import pathlib
    import fitz

    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    s3_hook = S3Hook(aws_conn_id="minio_airflow")
    
    import logging
    logger = logging.getLogger(__name__)

    BUCKET = 'papers-kg'
    def get_default_bucket():
        return BUCKET

    def get_corpus_base(CORPUS):
        return f'dags/{CORPUS}'
    
    def get_txt_raw_base(CORPUS):
        return f'{get_corpus_base(CORPUS)}/txt_raw'

    def download_pdf_file(file):
        if not os.path.exists(file):
            logger.info(f'downloading pdf: {file}') 
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
            logger.info(f'pdf file exists locally, skipping download: {file} ')

    def get_text_content(document):
        content = ""
        for page in document:
            content += page.get_text()
        return content
    
    def write_text_file(filename, content):
        pathlib.Path(filename).write_bytes(content.encode('utf-8').strip())
    
    def process_pdf_file(file):
        download_pdf_file(file)
        #license folder name+filename
        short_file_name = '/'.join(file.split('/')[-2:]).split('.pdf')[0]
        txt_file_name = f'{get_txt_raw_base(CORPUS)}/{short_file_name}.txt'
        file_exists_in_s3 = s3_hook.check_for_key(txt_file_name, get_default_bucket())
        if file_exists_in_s3:
            logger.info(f'txt file exists remotely, skipping conversion: {txt_file_name}')
        else:
            with fitz.open(file) as document:
                text = get_text_content(document)
                os.makedirs(txt_file_name.split(txt_file_name.split('/')[-1])[0], exist_ok=True)
                write_text_file(txt_file_name, text)
                logger.info(f'uploading to s3: {txt_file_name}')
                s3_hook.load_file(
    		        key=txt_file_name, 
    		        bucket_name=get_default_bucket(), 
    		        filename=txt_file_name, 
    		        replace=True, 
                ) 

    if CORPUS is None:
        CORPUS = params['CORPUS']
        logger.info(f"corpus: {CORPUS}")    

    logger.info(files)

    for file in files:
        process_pdf_file(file)