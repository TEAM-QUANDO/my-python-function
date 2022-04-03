from adlfs import AzureBlobFileSystem
import numpy as np
import cv2
import matplotlib.pyplot as plt
import dask
import dask.array as da
import dask.dataframe as dd
from typing import List
import urllib
import os


URL = "http://127.0.0.1:10000"
ACCOUNT_NAME = "devstoreaccount1"
KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="  # NOQA
CONN_STR = f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT_NAME};AccountKey={KEY};BlobEndpoint={URL}/{ACCOUNT_NAME};"  # NOQA

fs = AzureBlobFileSystem(account_name=ACCOUNT_NAME, 
                         connection_string=CONN_STR)

with fs.open("hello/66721840_10157452826804630_4325995290797539328_n.jpg", mode="rb") as src:
    res = src.read()
    
img = np.frombuffer(res, np.uint8)
img = cv2.imdecode(img, cv2.IMREAD_COLOR)
img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

plt.figure(figsize=(5,5))
plt.imshow(img)
plt.show()


def split_filepath(path: str):
    path = path.replace(os.sep, "/")
    dir, filename = os.path.split(path)
    return path, dir, filename

def parse_path(path: str):
    path = path.replace(os.sep, "/")
    return path

def parse_path_download_filelist(rfilepaths: List[str], lpath: str = "./tmp"):
    rfilepaths = [split_filepath(rpath) for rpath in rfilepaths]
    res = []
    for rpath, _, rfilename in rfilepaths:
        download_file_path = parse_path(f"{lpath}/{urllib.parse.quote(rfilename)}")
        res.append((rpath, download_file_path))
    return res

@dask.delayed
def read_files(fs: AzureBlobFileSystem, rpath: str, lpath: str):
    return fs.download(rpath, lpath)

rfilepaths = fs.ls("group-351")
download_list = parse_path_download_filelist(rfilepaths, lpath="E:\\temp")
futures = [read_files(fs, x[0], x[1]) for x in download_list]
_ = dask.compute(futures, scheduler='threads')