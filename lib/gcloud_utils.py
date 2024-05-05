import os
import re

from google.cloud import storage

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import logging
logger = logging.getLogger(__name__)

client = storage.Client.create_anonymous_client()
bucket = client.bucket('arxiv-dataset')

'''
paper_id must contain the required version e.g. 2211.00350v3
'''
def google_cloud_download(paper_id, file_name):
    year = paper_id.split('.')[0]
    try:
        blob = bucket.blob(f"arxiv/arxiv/pdf/{year}/{paper_id}.pdf")
        blob.download_to_filename(file_name)
    except Exception as e:
        print(e)
        #older paper have a 9 character scheme e.g. 1411.4555
        no_version = paper_id.split('v')[0]
        print(f'Trying: {no_version}v1')
        # version declared but not available for download, try v1; 
        # TODO perhaps find the most recent available version
        try:
            blob = bucket.blob(f"arxiv/arxiv/pdf/{year}/{no_version}v1.pdf")
            blob.download_to_filename(file_name)
        except Exception as e1:
            print(e1)
            #try without the version
            try:
                blob = bucket.blob(f"arxiv/arxiv/pdf/{year}/{no_version}.pdf")
                blob.download_to_filename(file_name)
            except Exception as e2:
                print(e2)
                raise e2

def download_paper_google(paper, BUCKET, PDF_BASE):

    if paper:
        s3_hook = S3Hook(aws_conn_id="minio_airflow")

        paper_title = re.sub('[^a-zA-Z0-9]', '_', paper['title'])
        short_id = f'{paper["id"][:10]}{paper["latest_version"]}'
        if 'license' in paper:
            if 'nonexclusive-distrib' in paper['license']:
                long_file_name = f"{PDF_BASE}/nonexclusive-distrib/{short_id}.{paper_title}.pdf"
            elif 'by-nc-nd' in paper['license']:
                long_file_name = f"{PDF_BASE}/by-nc-nd/{short_id}.{paper_title}.pdf"
            elif 'by-sa' in paper['license']:
                long_file_name = f"{PDF_BASE}/by-sa/{short_id}.{paper_title}.pdf"
            elif 'by' in paper['license']:
                long_file_name = f"{PDF_BASE}/by/{short_id}.{paper_title}.pdf"
            elif 'publicdomain' in paper['license']:
                long_file_name = f"{PDF_BASE}/publicdomain/{short_id}.{paper_title}.pdf"
            else:
                logger.info(f'**************** LICENSE {paper["license"]}')
                long_file_name = f"{PDF_BASE}/{short_id}.{paper_title}.pdf"
                raise Exception('must handle this license')
        else:
            long_file_name = f"{PDF_BASE}/{short_id}.{paper_title}.pdf"
        #prepare the folders for downloading
        os.makedirs(long_file_name.split(long_file_name.split('/')[-1])[0], exist_ok=True)

        file_exists_in_s3 = s3_hook.check_for_key(long_file_name, BUCKET)
        if(file_exists_in_s3):
            logger.info(f'File exists in s3. Skipping download/upload for: {long_file_name}')
        else:
            if os.path.exists(long_file_name) and (os.path.getsize(long_file_name) > 0) :
                    print(f'{short_id} file exists locally. Skipping download for: {long_file_name}')
            else:
                print(f'{short_id} Downloading {long_file_name}')
                google_cloud_download(short_id, long_file_name)

            #only upload to s3 if the size of the paper is > 0
            if(os.path.getsize(long_file_name) > 0):
                #upload to s3
                s3_hook.load_file(
    		        key=long_file_name, 
    		        bucket_name=BUCKET, 
    		        filename=long_file_name, 
    		        replace=True, 
                )
            else:
                print(f'{short_id} File is 0 bytes. Not uploading to s3.')
                # delete the 0 bytes file
                os.remove(long_file_name)
                raise Exception('{short_id} File is 0 bytes. Not uploading to s3.')



