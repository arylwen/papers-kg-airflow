from llama_index.core import KnowledgeGraphIndex
from llama_index.core import StorageContext
from llama_index.core import ServiceContext
from llama_index.core import load_index_from_storage

from llama_index.core.graph_stores import SimpleGraphStore 
from llama_index.core.langchain_helpers.text_splitter import SentenceSplitter
#from llama_index.core.node_parser import SimpleNodeParser

from llama_kg.llm_predictor.KronOpenAILLM import KronOpenAI
from llama_kg.llm_predictor.KronLLMPredictor import KronLLMPredictor

from llama_kg.readers import S3ListReader

from pyvis.network import Network

import logging
logger = logging.getLogger(__name__)

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
s3_hook = S3Hook(aws_conn_id="minio_airflow")

def index_file(document, index, index_html_base, fs):
    logger.info(f'Indexing {document}')
    index.insert(document = document, show_progress = True)
    save_pyvis_network_graph(index, document.id_, index_html_base, fs)


def init_index(persist_path, service_context, prompt, fs):
    if not fs.exists(persist_path):
        logger.info(f'No KGIndex found for {persist_path}, creating new empty index.')
        graph_store = SimpleGraphStore()
        storage_context = StorageContext.from_defaults(graph_store=graph_store)
        index = KnowledgeGraphIndex(
            [],
            max_triplets_per_chunk=2,
            storage_context=storage_context,
            service_context=service_context,
            kg_triple_extract_template=prompt,
        )
        index.storage_context.persist(persist_dir=persist_path, fs=fs)
    else:
        print(f'Loading index from {persist_path}')
        # rebuild storage context
        storage_context = StorageContext.from_defaults(persist_dir=persist_path, fs=fs)
        # load index
        index = load_index_from_storage(storage_context=storage_context, 
                                    service_context=service_context, 
                                    max_triplets_per_chunk=2,
                                    kg_triple_extract_template=prompt,
                                    show_progress = True)
    return index


def get_service_context(model):
    # define LLM
    llm=KronOpenAI(temperature=0.01, model=model, timeout=600)
    #chunk_size+prompt_length+expected length of returned triples must be less than max_tokens
    llm.max_tokens = 512 #192-48 - 512 because of the additional instruction in the prompt
    llm_predictor = KronLLMPredictor(llm)
    print(llm_predictor.metadata)

    # define TextSplitter
    text_splitter = SentenceSplitter(chunk_size=192, chunk_overlap=48, paragraph_separator='\n')

    #define NodeParser
    #node_parser = SimpleNodeParser(text_splitter=text_splitter)
    node_parser = text_splitter

    #define ServiceContext
    service_context = ServiceContext.from_defaults(llm_predictor=llm_predictor, node_parser=node_parser)

    return service_context

def load_documents(file_list):

    loader = S3ListReader(
                    bucket='papers-kg', 
                    keys = file_list, 
                    filename_as_id = True,
                    aws_access_id=s3_hook.conn_config.aws_access_key_id, 
                    aws_access_secret=s3_hook.conn_config.aws_secret_access_key,
                    s3_endpoint_url = s3_hook.conn_config.endpoint_url,
    #                file_metadata = lambda x: {'license': license},
                )
    documents = loader.load_data()
    documents = update_documents(documents, file_list)
    return documents

def get_license(file_name, file_list):
    short_name= file_name.split('/')[-1]
    for file in file_list:
        if short_name in file:
            return file.split('/')[-2]

# update metadata and exclusions
def update_documents(documents, file_list):
    for document in documents:
        # retain the filename
        document.id_ = document.id_.split('/')[-1].split('.txt')[0]
        license = get_license(document.id_, file_list)
        document.metadata = {'license': license}
        document.excluded_embed_metadata_keys = ['license']
        document.excluded_llm_metadata_keys = ['license']
    return documents

## create pyvis graph
## use generate_html with a s3 write
def save_pyvis_network_graph(index, file_name, index_html_base, fs):
    #display all nodes
    g = index.get_networkx_graph(limit = 60000)
    net = Network(height='1000px', width='100%', notebook=True, cdn_resources="in_line", directed=True)
    net.from_nx(g)
    html_name = f'{index_html_base}/{file_name}.html'
    html = net.generate_html(html_name)
    fs.write_text(html_name, html)