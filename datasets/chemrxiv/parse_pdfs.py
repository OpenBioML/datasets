import os
import pandas as pd
from tqdm import tqdm
import fitz
import multiprocessing as mp
from parse_ocr import detect_ocr
import glob


def get_text(doc):
    text_list = []

    doc = fitz.open(stream=doc)
    for i in range(doc.page_count):
        page = doc.load_page(i)
        text = page.get_text()
        if text != '':
            text_list.append(text)
        else:
            text = detect_ocr(page.get_pixmap())
            text_list.append(text)
    if len(text_list) > 0:
        return '\n'.join(text_list)


def process_part(files, output_dir, st):
    i = 0
    data = {
        'TEXT': [],
        'SOURCE': []
    }
    for obj in tqdm(files):
        key = obj.key
        try:
            body = obj.get()['Body'].read()
            text = get_text(body)
            if text is not None:
                data['TEXT'].append(text)
                data['SOURCE'].append(key)
                i += 1
                if i > 0 and i % 10 == 0:
                    pd.DataFrame(data).to_parquet(
                        f'{output_dir}/{st}_{i}.parquet', index=False)
                    data = {
                        'TEXT': [],
                        'SOURCE': []
                    }
        except:
            # print(key)
            continue


def process_part_files(files, output_dir, st):
    i = 0
    data = {
        'TEXT': [],
        'SOURCE': []
    }
    for f in tqdm(files):
        key = f
        try:
            with open(f, 'rb') as f_handle:
                body = f_handle.read()
            text = get_text(body)
            if text is not None:
                data['TEXT'].append(text)
                data['SOURCE'].append(key)
                i += 1
                if i > 0 and i % 10 == 0:
                    pd.DataFrame(data).to_parquet(
                        f'{output_dir}/{st}_{i}.parquet', index=False)
                    data = {
                        'TEXT': [],
                        'SOURCE': []
                    }
        except Exception as err:
            print(err)
            os.remove(f)
            continue


if __name__ == '__main__':
    output_dir = 'chemrxiv_text'
    os.makedirs(output_dir, exist_ok=True)
    files = glob.glob('CHEMRXIV_pdfs/*.pdf')
    N = len(files)
    print(N)
    processes = []
    num_process = 10
    rngs = [(i*int(N/num_process), (i+1)*int(N/num_process))
            for i in range(num_process)]
    print(rngs)
    for rng in rngs:
        start, end = rng
        p = mp.Process(target=process_part_files, args=[
            files[start:end], output_dir, start])
        p.start()
        processes.append(p)
    for p in processes:
        p.join()
