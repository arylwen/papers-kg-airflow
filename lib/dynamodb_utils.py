import os
import json
import pandas as pd
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

import logging
logger = logging.getLogger(__name__)

#__LOCAL__ = True

#if(__LOCAL__):
#    dynamodb = boto3.resource('dynamodb', endpoint_url='http://10.0.0.179:31942') 
#else:
#    session = boto3.Session(region_name='us-west-2')
#    dynamodb = session.resource('dynamodb')

class ArxivMetadata:
    """Encapsulates an Amazon DynamoDB table of request data."""
    def __init__(self, dyn_resource):
        """
        :param dyn_resource: A Boto3 DynamoDB resource.
        """
        self.dyn_resource = dyn_resource
        self.table = None

    def exists(self, table_name):
        """
        Determines whether a table exists. As a side effect, stores the table in
        a member variable.
        :param table_name: The name of the table to check.
        :return: True when the table exists; otherwise, False.
        """
        try:
            table = self.dyn_resource.Table(table_name)
            table.load()
            exists = True
        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                exists = False
            else:
                logger.error(
                    "Couldn't check for existence of %s. Here's why: %s: %s",
                    table_name,
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        else:
            self.table = table
        return exists
    
    def create_table(self, table_name):
        """
        Creates an Amazon DynamoDB table that can be used to store request data.
        The table uses the article submission {yearmonth} as hash key and the article id as sort key.
        :param table_name: The name of the table to create.
        :return: The newly created table.
        """
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'yearmonth', 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': 'id', 'KeyType': 'RANGE'}  # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'yearmonth', 'AttributeType': 'S'},
                    {'AttributeName': 'id', 'AttributeType': 'S'},
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10})
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return self.table
        
    def get_paper_metadata(self, paper_id):
        pid = paper_id[:10]
        yearmonth = pid.split('.')[0]
        logger.info(f'{yearmonth}  {pid}')
        response = self.table.query(
            KeyConditionExpression=Key('yearmonth').eq(yearmonth) & Key('id').eq(pid) )
        logger.info(f'{response}')
        return response

        
def get_arxiv_metadata():
    table_name = 'arxiv-metadata'
    session = boto3.Session(region_name='us-west-2')
    dynamodb = session.resource('dynamodb', 
                                endpoint_url='http://10.0.0.179:31942',
                                aws_access_key_id='abc123', 
                                aws_secret_access_key='abc123'
                                ) 
    arxiv_metadata = ArxivMetadata(dynamodb)
    arxiv_metadata_exists = arxiv_metadata.exists(table_name)
    if not arxiv_metadata_exists:
        print(f"\nCreating table {table_name}...")
        arxiv_metadata.create_table(table_name)
        print(f"\nCreated table {arxiv_metadata.table.name}.")
    return arxiv_metadata

def dynamodb_get_paper_metadata(paper_id):
    arxiv_metadata = get_arxiv_metadata()
    #metadata = arxiv_metadata.get_paper_metadata(paper_id)['Items']
    #logger.info(metadata)
    papers =  arxiv_metadata.get_paper_metadata(paper_id)['Items']
    logger.info(f'{papers}')
    if len(papers) > 0:
        return papers[0] 
    else:
        return {}






