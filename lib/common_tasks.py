
from airflow.decorators import task

@task
def batch_splitter(article_ids):
    n = len(article_ids)//1000 + 1
    batches = [article_ids[i:i + n] for i in range(0, len(article_ids), n)]

    return batches
