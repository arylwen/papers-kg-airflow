import math
from airflow.decorators import task

'''
* airflow allows up to 1024 mapped tasks
* tasks are heavy constructs in terms of performance
* we want to minimize the number of tasks created while keeping their number under 1024
* this function supports mapping 1024*1024 tasks, which is sufficient for this application
'''
@task
def batch_splitter(article_ids): 
    mapped_tasks = int(math.sqrt(len(article_ids)))
    n = len(article_ids)//mapped_tasks + 1
    batches = [article_ids[i:i + n] for i in range(0, len(article_ids), n)]

    return batches
