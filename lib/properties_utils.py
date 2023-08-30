def load_properties(filepath, sep='=', comment_char='#'):
    '''
    Read the file passed as parameter as a properties file.
    '''
    props = {}
    with open(filepath, "rt") as f:
        for line in f:
            l = line.strip()
            if l and not l.startswith(comment_char):
                key_value = l.split(sep)
                key = key_value[0].strip()
                value = sep.join(key_value[1:]).strip().strip('"') 
                props[key] = value 
    return props
'''
Save a dictionary as a properties file; use to remember the latest processed id.
TODO store comments
'''
def save_properties(properties, filepath, sep='=', comment_char='#'):
    with open(filepath, 'w') as f: 
        for key, value in properties.items(): 
            f.write('%s %s %s\n' % (key, sep, value))