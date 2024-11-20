import requests
import os


def extract(url="https://shorturl.at/5YexG", 
            file_path="data/heart-failure.csv",
            directory="data",
):
    """"Extract a url to a file path"""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with requests.get(url) as r:
        with open(file_path, 'wb') as f:
            f.write(r.content)
    return file_path

if __name__ == '__main__':
    extract()