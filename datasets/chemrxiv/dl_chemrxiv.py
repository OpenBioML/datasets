import os
import pandas as pd
from multiprocessing.pool import ThreadPool
import requests


def download(URL, out_dir):
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
    }
    doc = requests.get(URL, headers=headers)
    file_name = URL.split('/')[-1]
    file_name = f'{out_dir}/{file_name}'
    try:
        with open(file_name, 'wb') as f:
            f.write(doc.content)
    except:
        print(URL)


if __name__ == '__main__':
    output_dir = 'CHEMRXIV_pdfs'
    os.makedirs(output_dir, exist_ok=True)
    df = pd.read_parquet('chemrxiv.parquet')['asset']

    done_urls = os.listdir('CHEMRXIV_pdfs')

    urls = df.values

    num_proc = 10
    with ThreadPool(num_proc) as p:
        p.starmap(download, [(url, output_dir) for url in urls])
