import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

import re
import time
import json
import urllib

import pandas as pd

from gcloud_utils import google_cloud_download

from xml2json_utils import xml2json
from dynamodb_utils import dynamodb_get_paper_metadata

import logging
logger = logging.getLogger(__name__)


def dynamodb_metadata_search(paper_id):
    paper_metadata = dynamodb_get_paper_metadata(paper_id)
    paper = None
    try:
        paper = {}
        paper['id'] = paper_id
        paper['title'] = paper_metadata['title']
        paper['abstract'] = paper_metadata['abstract']
        paper['license'] = paper_metadata['license']
        paper['versions'] = paper_metadata['versions']
        latest_version = 'v1'
        for version in paper['versions'] :
            #v = json.loads(version)
            if version['version'] > latest_version:
                latest_version = version['version']
        paper['latest_version'] = latest_version
    except (KeyError, IndexError) as ie :
        paper = None
        logger.info(ie)
        logger.info(f'Paper {paper_id} not found. Perhas should download a new metadata db version?')
    
    return paper

'''
  assumes metadata files in dags
  TODO download latest metadata
'''
def load_metadata_df():
    nl = 0
    metadata_records = []
    with open("arxiv-metadata-oai-snapshot.json") as f1:
        for line in f1:
            #print(line)   
            metadata_record = json.loads(line)
            #print(metadata_record)
            metadata_records.extend([metadata_record])
            #nl+=1
            #if (nl == 5): break

    #print(metadata_records)
    metadata_df = pd.DataFrame(metadata_records)
    metadata_df.shape

    return metadata_df

# search for paper in the metadata_df
def kaggle_search(paper_id, metadata_df):
    row = metadata_df.loc[metadata_df['id'] == paper_id]
    #print(row)
    paper = None
    try:
        paper = {}
        paper['id'] = row['id'].values[0]
        paper['title'] = row['title'].values[0]
        paper['versions'] = row['versions'].values[0]
        paper['abstract'] = row['abstract'].values[0]

        latest_version = 'v1'
        for version in paper['versions'] :
            #v = json.loads(version)
            if version['version'] > latest_version:
                latest_version = version['version']
        paper['latest_version'] = latest_version
    except IndexError as ie:
        print(ie)
        print(f'Paper {paper_id} not found. Perhas should download a new metadata db version?')
    
    return paper

def download_paper_kaggle(paper_id, downloaded_article_ids, PDF_BASE):
    #global i
    #global downloaded_article_ids
    #i = i+1
    if(paper_id):
        if paper_id[:10] in downloaded_article_ids:
            print(f'A version of {paper_id} exists.')
        else:
            paper = kaggle_search(paper_id[:10])
            if paper:
                paper_title = re.sub('[^a-zA-Z0-9]', '_', paper['title'])
                short_id = f'{paper["id"]}{paper["latest_version"]}'
                long_file_name = f"{PDF_BASE}/{short_id}.{paper_title}.pdf"
                file_name = f"{short_id}.{paper_title}.pdf"
                if(os.path.exists(long_file_name)):
                    print(f'{paper_id} File exists. Skipping {file_name}')
                else:
                    print(f'{paper_id} Downloading {file_name}')
                    # this might hit arxiv's rate limits
                    google_cloud_download(short_id, long_file_name)
                    time.sleep(5)
            else:
                # TODO missed papers - write them down in a file for later download
                print(f'Paper {paper_id} not in metadata, probably not on gcloud yet.')
                google_cloud_download(paper_id, paper_id)
                time.sleep(5)

'''
   paper_id - string
'''
def get_paper_metadata_arxiv(paper_id):
    usock = urllib.request.urlopen('http://export.arxiv.org/api/query?id_list='+paper_id)
    #xmldoc = minidom.parse(usock)
    xmldoc = usock.read()
    usock.close()
    logger.info(f'--------------------{xmldoc}')
    json_str = xml2json(xmldoc)
    logger.info(f'--------------------{json_str}')

    metadatada_json = json.loads(json_str)
    return metadatada_json

def arxiv_metadata_search(paper_id):
    paper_metadata = get_paper_metadata_arxiv(paper_id)
    paper = None
    try:
        paper = {}
        paper['id'] = paper_id
        paper['title'] = paper_metadata['feed']['entry']['title']
        paper['abstract'] = paper_metadata['feed']['entry']['summary']

        #paper['versions'] = row['versions'].values[0]
        #latest_version = 'v1'
        #for version in paper['versions'] :
            #v = json.loads(version)
        #    if version['version'] > latest_version:
        #        latest_version = version['version']
        paper['latest_version'] = paper_metadata['feed']['entry']['id'].split('/')[-1]
    except IndexError as ie:
        print(ie)
        print(f'Paper {paper_id} not found. Perhas should download a new metadata db version?')
    
    return paper