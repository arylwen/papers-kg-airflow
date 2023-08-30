import os
import re
import pathlib

def read_text_file_utf(filename):
    text_bytes = pathlib.Path(filename).read_bytes()
    print(len(text_bytes))
    text_bytes = text_bytes.decode(encoding = 'utf-8')
    return text_bytes

def read_text_file_ascii(filename):
    text = pathlib.Path(filename).read_bytes().decode(encoding = 'utf-8')
    text = text.encode('ascii', 'ignore').decode('ascii')
    return text

def write_text_file_utf(filename, content):
    pathlib.Path(filename).write_bytes(content.encode('utf-8').strip())

def write_text_file_ascii(filename, content):
    pathlib.Path(filename).write_bytes(content.encode('ascii').strip()) 

'''
    - split words
    - latex encoded sequences
    - utf specials as fi, ffi
'''
def utf_cleanup(raw_text):
    text = raw_text
    #----------------- general text cleanup ---------------------------
    # convert split words on line break, e.g. post-\nediting
    text = text.replace('-\n', '')
    # encoded sequences will always generate sequences larger than the chunk size
    latexit = r'<latexit.*latexit> *'
    text = re.sub(latexit, ' ', text)
    # sepcial characters part of words
    #fi  
    special_characters = r'[\ufb01]'
    text = re.sub(special_characters, 'fi', text)
    #ffi 
    special_characters = r'[\ufb03]'
    text = re.sub(special_characters, 'ffi', text)
    #ﬀ
    special_characters = r'[ﬀ]'
    text = re.sub(special_characters, 'ff', text)
    #ﬂ
    special_characters = r'[ﬂ]'
    text = re.sub(special_characters, 'fl', text)

    return text


def cleanup_text(raw_text):
    text = raw_text

    #----------------- general text cleanup ---------------------------
    # convert split words on line break, e.g. post-\nediting
    #text = text.replace('-\n', '')

    # encoded sequences will always generate sequences larger than the chunk size
    #latexit = r'<latexit.*latexit> *'
    #text = re.sub(latexit, ' ', text)
    # remove numbers
    #numbers = r'[0-9]'
    numbers = r'\b\d+\b'
    text = re.sub(numbers, '', text)
    # remove urls - not needed for KG
    text = re.sub(r'http\S+', '', text, flags=re.MULTILINE)
    # remove email addresses - not needed for KG
    text = re.sub(r'\S*@\S*\s?', '', text)
    # remove lines less than 20 characters  ----too wide net: ^.{1,20}$
    text = re.sub(r'^[\w\-\s():<>?,]{1,45}$', '', text, flags=re.MULTILINE)
    # remove single dots
    text = re.sub(r'^[.]$', '', text, flags=re.MULTILINE)
    # remove words longer than 20 characters, but not paragraph lines (ending with .) 
    text = re.sub(r'\b\w{20,}\b', '', text)    
    # remove empty lines
    text = re.sub(r'\n{2,}', '\n', text)    

    #--------------- arxiv specific text cleanup ----------------------------------
    # remove references
    text = re.sub(r'doi\:.*\n?', '\n', text, flags=re.MULTILINE)
    text = re.sub(r'abs\/.*\n?', '\n', text, flags=re.MULTILINE)
    text = re.sub(r'URL\:.*\n?', '\n', text, flags=re.MULTILINE)
    text = re.sub(r'url\:.*\n?', '\n', text, flags=re.MULTILINE)
    text = re.sub(r'arXiv\:.*\n?', '\n', text, flags=re.MULTILINE)
    # remove Figure captions:
    text = re.sub(r'(Figure [0-9]*:*)', ' ', text)
    # remove Table captions:
    text = re.sub(r'(Table [0-9]*:*)', ' ', text)
    # no code and math ,.- have special treatment
    special_characters = r'[\!\"\#\$\%\&\*\+\/\:\;\<\=\>\?\\\^\_\|\(\)\[\]\{\}]'
    text = re.sub(special_characters, ' ', text)
    # single-letter-comma
    special_characters = r'\b(\w,)'
    text = re.sub(special_characters, ' ', text)
     # multiple commas
    special_characters = r'(,{2,} {0,})|(, {1,})'
    text = re.sub(special_characters, ',', text)
    special_characters = r'(,{2,} {0,})|(, {1,})'
    text = re.sub(special_characters, ',', text)
    # single-letter-space
    special_characters = r'\b(\w\s)'
    text = re.sub(special_characters, ' ', text)
    # single-letter-space
    special_characters = r'\b(\w\.)'
    text = re.sub(special_characters, '.', text)
    # dagling dashes
    #special_characters = r'(\ \-\b)|(\b- )|( - )'
    #text = re.sub(special_characters, ',', text)
    # multiple spaces
    special_characters = r'( {2,})'
    text = re.sub(special_characters, ' ', text)
    # multiple .
    special_characters = r'(\.{2,})'
    text = re.sub(special_characters, '.', text)

    # remove lone new lines in the middle of the sentence - leave only the new lines after .(dot)
    one_new_line = r'(?<![\.\n])\n(?!\n)'
    text = re.sub(one_new_line, ' ', text)

    # dagling dashes - after lone new lines to not lose compose words
    special_characters = r'(\ \-\b)|(\b- )|( - )'
    text = re.sub(special_characters, ',', text)

    # CC AA BB GCRE2 SFR
    text = re.sub(r'(\b[A-Z]{1,3}[0-9]*[A-Z]{0,3}\b\s){3,}', ' ', text)

    #stray punctuation
    text = re.sub(r'(\, {1,}\,?\.)', ' ', text)

    # stray character at the begining of the line
    special_characters = r'(\n\. )|(\n\- )|(\n {1,})'
    text = re.sub(special_characters, '\n', text)
 
    text = re.sub(r'(\,\.)', '.', text)
    text = re.sub(r'(\. ?){1,}', '.', text)

    # multiple spaces
    special_characters = r'( {2,})'
    text = re.sub(special_characters, ' ', text)

    #multiple '. '
    text = re.sub(r'(\. ){2,}', ' ', text)

    #lone dots on a line '. '
    text = re.sub(r'\n *\. *\n', '\n', text)

    #stray punctuation
    text = re.sub(r'(\-\.)', '', text)

    # stray character at the begining of the line
    special_characters = r'(\n\. )|(\n\- )|(\n {1,})'
    text = re.sub(special_characters, '\n', text)

    #math
    special_characters = r'(\bdx\b)|(\bdy\b)|(\bdx2\b)'
    text = re.sub(special_characters, '', text)

    #things
    special_characters = r'(\.and\b)'
    text = re.sub(special_characters, 'and', text)
    text = re.sub(r'CoRR', '', text)
    text = re.sub(r'URL', '', text)

    #too many commas
    text = re.sub(r'( ,){2,}', ', ', text)
    #space befoe dot
    text = re.sub(r'( \.)', '.', text)

    # remove lines less than 40 characters; keep sentences 
    text = re.sub(r'^[\w\-\s():<>?,]{1,40}$', '', text, flags=re.MULTILINE)

    #lone dots on a line - good paragraph breaks?
    text = re.sub(r'(\n *\.+ *)', '\n\n', text, flags=re.MULTILINE)

    # Academic stopwords
    words = r'(Furthermore)|(Moreover)|(However)|(What)|(Overall)|(Nonetheless)|(Although)|(particularly)|(Essentially)'
    text = re.sub(words, '', text)
    words = r'(Recently)|(Particularly)|(Additionally)|(Finally)|(Now)|(Both)|(Secondly)|(In general)'
    text = re.sub(words, '', text)
    words = r'(\bOur\b)|(\bThe\b)|(\bthe\b)|(\balso\b)|(wo cond\.)|(arXiv preprint)|(IEEE\.)|(arXiv)'
    text = re.sub(words, '', text)
    # stray character at the begining of the line after removing academic stopwords
    special_characters = r'(\n\. )|(\n\- )|(\n {1,})'
    text = re.sub(special_characters, '\n', text)
    # multiple spaces
    special_characters = r'( {2,})'
    text = re.sub(special_characters, ' ', text)
    #arxiv coref hack: we shows up as topic; match only full words
    words = r'(\bWe\b)'
    text = re.sub(words, 'Authors', text)
    words = r'(\bwe\b)'
    text = re.sub(words, 'authors', text)
  
    return text


def cleanup_file(raw_file_name, clean_file_name):
    #dirname = os.path.dirname(raw_file_name)
    #basename = os.path.basename(raw_file_name)
    #ascii_file_name = clean_file_name
    text = read_text_file_utf(raw_file_name)
    # replace the fi, ffi, fl etc...
    text = utf_cleanup(text)
    # convert to ascii - we could ignore the other non-ascii
    text = text.encode('ascii', 'ignore').decode('ascii')
    # rest of cleanup
    text = cleanup_text(text)
    write_text_file_ascii(clean_file_name, text)
