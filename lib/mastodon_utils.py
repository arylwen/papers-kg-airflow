import re
import json
import requests

import pandas as pd
import numpy as np

def user_lookup(acct):
    URL = f'https://mastodon.social/api/v1/accounts/lookup'
    params = {
        'acct': acct
    }

    r = requests.get(URL, params=params)
    user = json.loads(r.text)
    
    return user

'''
Some toots are reblogs; we would need to bring their content to the content column for normalization.
'''
def update_content(row):
    #print(row)
    reblog_dict = row['reblog']
    if reblog_dict and ('content' in reblog_dict):
        row['content'] += reblog_dict['content']
    return row

# extract article id to use for download
ARXIV_PREFIX_1 = "https://arxiv.org/"
ARXIV_PREFIX_2 = "arxiv.org/"
ARXIV_PREFIX_3 = "arXiv"

def get_article_id(col_str):
    #print(col_str)
    if col_str is np.nan:
         print(f'Not an arxiv article: {col_str}')
         return
    if col_str.startswith(ARXIV_PREFIX_1) | col_str.startswith(ARXIV_PREFIX_2):
        #an arxiv article
        article_id = col_str.split('/')[-1]
        #some articles have an extension
        article_id = '.'.join(article_id.split('.')[:2])
        articles = re.findall(r'[0-9]{4,4}\.[0-9]+[a-zA-Z]*[0-9]*', article_id)
        #print(articles)
        article_id = None
        if(len(articles) > 0):
            article_id = articles[0]
        else:
            print(f'{col_str} not an arxiv article.')
        return article_id
    elif col_str.startswith(ARXIV_PREFIX_3):
        #an arxiv article
        article_id = col_str.split(':')[-1]
        #some article have an extension
        article_id = '.'.join(article_id.split('.')[:2])
        return article_id
    elif re.search(r'[0-9]{4,4}\.[0-9]+[a-zA-Z]*[0-9]*', col_str):
        articles = re.findall(r'[0-9]{4,4}\.[0-9]+[a-zA-Z]*[0-9]*', col_str)
        #print(articles)
        article_id = None
        if(len(articles) > 0):
            article_id = articles[0]
        else:
            print(f'{col_str} not an arxiv article.')
        return article_id
    else:
        print(f'Not an arxiv article: {col_str}')

def new_toots_to_csv(run_id, user_id, CORPUS, CORPUS_BASE, LATEST):
    URL = f'https://mastodon.social/api/v1/accounts/{user_id}/statuses'
    params = {
       'limit': 40,
       'since_id':  LATEST
#      'min_id':  LATEST
    }

    results = []
    LATEST = 0

    while True:
        print(params)
        r = requests.get(URL, params=params)
        toots = json.loads(r.text)

        if len(toots) == 0:
            break
    
        results.extend(toots)
    
        max_id = toots[-1]['id']
        params['max_id'] = max_id
        if(LATEST == 0):
            # remember the highest toot id processed
            LATEST = toots[0]["id"]
        print(f'first:{toots[0]["id"]} last:{max_id}')
    
    df = pd.DataFrame(results)
    print(f'Latest: {LATEST}; Total new toots: {df.shape[0]}')
    #print(df.head(2))
    
    if df.shape[0] > 0:
        # fix content column
        df = df.apply(lambda row: update_content(row), axis = 1)
        # extract links - limit to arxiv links
        pattern = r'((https?:\/\/(?:www\.)?)?(arxiv)\.[a-zA-Z0-9()]{1,6}[-a-zA-Z0-9()@:%_+.~#?&/=]{2,256})|[0-9]{4,4}\.[0-9]+[a-zA-Z]*[0-9]*'
        df['links'] = df["content"].str.extract(pattern, expand=True)[0]
        # extract article id
        df['article_id'] = df['links'].apply(get_article_id)
        # save list of new toots
    csv_name = f"{CORPUS_BASE}/pending/{run_id}_{CORPUS}.csv"
    df.to_csv(csv_name, index=False)
    
    return csv_name, LATEST, df.shape[0]

def toots_to_csv(run_id, user_id, CORPUS, CORPUS_BASE, START_AT, END_AT):
    URL = f'https://mastodon.social/api/v1/accounts/{user_id}/statuses'
    params = {
       'limit': 40,
       'since_id':  START_AT
    }

    results = []
    # START_AT could be arbitrary; LATEST_TOOT is smallest toot id in corpus greater than START_AT
    LATEST_TOOT = 0
    max_id = 0

    while max_id < int(END_AT):
        print(params)
        r = requests.get(URL, params=params)
        toots = json.loads(r.text)

        if len(toots) == 0:
            break
    
        results.extend(toots)
    
        max_id = toots[-1]['id']
        params['max_id'] = max_id
        if(LATEST_TOOT == 0):
            # remember the highest toot id processed before this batch
            LATEST_TOOT = toots[0]["id"]
        print(f'first:{toots[0]["id"]} last:{max_id}')
    
    df = pd.DataFrame(results)
    print(f'Latest: {max_id}; Total new toots: {df.shape[0]}')
    
    if df.shape[0] > 0:
        # fix content column
        df = df.apply(lambda row: update_content(row), axis = 1)
        # extract links - limit to arxiv links
        pattern = r'((https?:\/\/(?:www\.)?)?(arxiv)\.[a-zA-Z0-9()]{1,6}[-a-zA-Z0-9()@:%_+.~#?&/=]{2,256})|[0-9]{4,4}\.[0-9]+[a-zA-Z]*[0-9]*'
        df['links'] = df["content"].str.extract(pattern, expand=True)[0]
        # extract article id
        df['article_id'] = df['links'].apply(get_article_id)
        # save list of new toots
    csv_name = f"{CORPUS_BASE}/pending/{run_id}_{CORPUS}.csv"
    df.to_csv(csv_name, index=False)
    
    return csv_name, max_id, df.shape[0]