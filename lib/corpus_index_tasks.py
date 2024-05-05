import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

import logging
logger = logging.getLogger(__name__)

import subprocess
import sys
import pkg_resources

def package_installed(package_name):
    installed = False
    try:
        package_info = pkg_resources.get_distribution(package_name)
        installed = True
    except:
        installed = False

    return installed

def install(package_name):
    if package_installed(package_name):
        logger.info(f'{package_name} installed, skipping installation')
    else:
        logger.info(f'installing {package_name}')
        subprocess.check_call([sys.executable, "-m", "pip", "install", '--no-deps',package_name])

from airflow.decorators import task  #, task_group
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from s3_utils import list_files_from_s3
from common_tasks import batch_splitter

from dataset_utils import (
    get_default_bucket,
#    get_by_nc_nd,
    get_by_sa,
    get_by,
    get_nonexclusive_distrib,
    get_publicdomain,
    get_txt_cleaned_base,
)

#@task_group
def text_indexer_mapper(article_ids, CORPUS ):
    batches = batch_splitter(article_ids)
    papers = index_files.partial(CORPUS=CORPUS).expand(files=batches)

    return papers

@task
def get_cleaned_txt_file_names(CORPUS, BUCKET=get_default_bucket(), params=None):

    if CORPUS is None:
        CORPUS = params['CORPUS']
        logger.info(f"corpus: {CORPUS}")

    # do not index the papers that do not allow derivative works TODO index the abstracts
    #file_list = list_files_from_s3(BUCKET, get_by_nc_nd(CORPUS, get_txt_cleaned_base))
    file_list = list_files_from_s3(BUCKET, get_by_sa(CORPUS, get_txt_cleaned_base)+'/')
    #logger.info(file_list)
    file_list.extend(list_files_from_s3(BUCKET, get_by(CORPUS, get_txt_cleaned_base)+'/'))
    #logger.info(file_list)
    file_list.extend(list_files_from_s3(BUCKET, get_nonexclusive_distrib(CORPUS, get_txt_cleaned_base)+'/'))
    #logger.info(file_list)
    file_list.extend(list_files_from_s3(BUCKET, get_publicdomain(CORPUS, get_txt_cleaned_base)+'/'))
    #logger.info(file_list)
    return file_list

from pathlib import Path
def load_reqs(file: str = '../requirements.txt'):
    with open(Path(__file__).parent / file) as f:
        return "\n".join(f.readlines())

@task.virtualenv(
    task_id="index_files_venv", requirements=load_reqs(), system_site_packages=True, max_active_tis_per_dagrun=1
)
#@task(task_id="index_files", max_active_tis_per_dagrun=1)
def index_files(files, CORPUS, params=None):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    import os
    import s3fs
    import logging
    logger = logging.getLogger(__name__)

    from llama_kg.index.base import (
    #from s3_index_utils import (
        index_file,
        init_index,
        get_service_context,
        load_documents,
    )

    from llama_index.core.prompts.base import Prompt
    from llama_index.core.prompts.prompt_type import PromptType

    # using local OpenAI compatible API TODO create generic connection
    import openai
    os.environ['OPENAI_API_KEY'] = "sk-48characterstofakeanopenaikey48charactersopenai0"
    os.environ['OPENAI_API_BASE'] = "http://10.0.0.179:30958/v1"
    openai.api_key = "sk-48characterstofakeanopenaikey48charactersopenai0"
    openai.api_base = "http://10.0.0.179:30958/v1"

    INDEX_MODEL = "Arylwen/instruct-palmyra-20b-gptq-8"

    PALMYRA_SHORT_INLINE_KG_PROMPT_TEMPLATE = (
            "Below is an instruction that describes a task, paired with an input that provides further context. "
            "Write a response that appropriately completes the request.\n\n"
            "### Instruction:\n"  
            "Some text is provided below. Given the text, extract up to {max_knowledge_triplets}  knowledge triplets in the form of " 
            "(subject, predicate, object). \n\n" 
            "Avoid duplicates. \n\n"  
            "### Input: \n"
            "Text: Alice is Bob's mother. \n" 
            "Triplets: \n"
            "    (Alice, is mother of, Bob) \n"
            "Text: Philz is a coffee shop founded in Berkeley in 1982. \n"
            "Triplets: \n"
            "    (Philz, is, coffee shop) \n"
            "    (Philz, founded in, Berkeley) \n"
            "    (Philz, founded in, 1982) \n"
            "### Text: {text} \n\n"
            "### Response:"
)

    PALMYRA_SHORT_INLINE_KG_TRIPLET_EXTRACT_PROMPT = Prompt(
        PALMYRA_SHORT_INLINE_KG_PROMPT_TEMPLATE, prompt_type=PromptType.KNOWLEDGE_TRIPLET_EXTRACT
    )

    prompt = PALMYRA_SHORT_INLINE_KG_TRIPLET_EXTRACT_PROMPT

    if CORPUS is None:
        CORPUS = params['CORPUS']
        logger.info(f"corpus: {CORPUS}")

    INDEX_NAME = f"{INDEX_MODEL.replace('/', '-')}-default-no-coref-{CORPUS}"
    #html-kg is the bucket name for html and indices-kg is the bucket name for indices
    HTML_FOLDER = f"html-kg/{INDEX_NAME}"
    # path to store the index in S3 
    INDEX_PERSIST_PATH = f"indices-kg/{INDEX_NAME}"

    logger.info(f'Indexing batch:\n {files}')

    s3_hook = S3Hook(aws_conn_id="minio_airflow")
    s3_fs = s3fs.S3FileSystem(
        key = s3_hook.conn_config.aws_access_key_id,
        secret = s3_hook.conn_config.aws_secret_access_key,
        endpoint_url= s3_hook.conn_config.endpoint_url,
    )

    service_context = get_service_context(INDEX_MODEL)

    #create or load index
    index = init_index(persist_path=INDEX_PERSIST_PATH, service_context=service_context, prompt=prompt, fs=s3_fs)
    logger.info(index.ref_doc_info)

    documents = load_documents(files)
    logger.info(f'Indexing documents:\n {documents}')
    
    def is_file_indexed(document, index):
        indexed_documents = index.ref_doc_info.keys()

        for doc in indexed_documents:
            if doc in document.id_:
                return True

        return False

    def index_cleaned_txt_file(document):

        file_indexed = is_file_indexed(document, index)
        if file_indexed:
            logger.info(f'txt file in index, skipping indexing: {document.id_}')
        else:
                logger.info(f'indexing: {document}')
                index_file(document,  index, HTML_FOLDER, s3_fs) 

    #save the index every hour or after each file; this task could take days and we need intermediary saves
    import time
    last_save = time.time()
    for document in documents:
        index_cleaned_txt_file(document)
        current = time.time()
        if (current - last_save) > 3600:
            logger.info(f'{current - last_save}s since last save. Persisting intermediary index: {INDEX_PERSIST_PATH}')
            index.storage_context.persist(persist_dir=INDEX_PERSIST_PATH, fs=s3_fs)
            last_save = current

    #persist index; this is safe because for this index only one task can run at a time
    logger.info(f'Persisting index: {INDEX_PERSIST_PATH}')
    index.storage_context.persist(persist_dir=INDEX_PERSIST_PATH, fs=s3_fs)
