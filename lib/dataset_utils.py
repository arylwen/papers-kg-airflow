
BUCKET = 'papers-kg'

def get_default_bucket():
    return BUCKET

def get_corpus_base(CORPUS):
    return f'dags/{CORPUS}'

def get_corpus_config(CORPUS):
    return f'{CORPUS}.properties'

def get_pdf_base(CORPUS):
    return f'{get_corpus_base(CORPUS)}/pdf'

def get_txt_raw_base(CORPUS):
    return f'{get_corpus_base(CORPUS)}/txt_raw'

def get_txt_cleaned_base(CORPUS):
    return f'{get_corpus_base(CORPUS)}/txt_cleaned'

'''
    CORPUS - name of the corpus
    base_function - document base function 
        values: get_pdf_base
                get_txt_raw_base
                get_txt_cleaned_base
'''
def get_by_nc_nd(CORPUS, base_function=get_pdf_base):
    return f'{base_function(CORPUS)}/by-nc-nd'

def get_by_sa(CORPUS, base_function=get_pdf_base):
    return f'{base_function(CORPUS)}/by-sa'

def get_by(CORPUS, base_function=get_pdf_base):
    return f'{base_function(CORPUS)}/by'

def get_nonexclusive_distrib(CORPUS, base_function=get_pdf_base):
    return f'{base_function(CORPUS)}/nonexclusive-distrib'

def get_publicdomain(CORPUS, base_function=get_pdf_base):
    return f'{base_function(CORPUS)}/publicdomain'