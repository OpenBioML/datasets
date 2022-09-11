import os
from gzip import decompress

import requests


def download_sdf(url, save_dir="data/"):
    filename = url.split("/")[-1].replace(".gz", "")
    filename = f"{save_dir}{filename}"

    if os.path.exists(filename):
        print(f"| {filename} already exists")
        return filename

    response = requests.get(url)
    sdf_content = decompress(response.content)

    save_to_file(filename, sdf_content)

    return filename


def save_to_file(filename, content):
    with open(filename, "wb") as f:
        f.write(content)
