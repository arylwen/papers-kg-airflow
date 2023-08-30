import urllib.request

# TODO use kaggle api https://github.com/Kaggle/kaggle-api
def download_remote_file(uri, target_path):
    with urllib.request.urlopen(uri) as file:
        with open(target_path, "wb") as new_file:
            new_file.write(file.read())