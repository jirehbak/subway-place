#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May  1 15:39:06 2020

@author: jireh.park
"""


import pandas as pd
import os
from tqdm import tqdm
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))+ '/key/level-district.json'

def list_blob(bucket_name):
    global credentials
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs())
    #blob = bucket.blob(destination_blob_name)

    #blob.upload_from_filename("tmp", content_type='text/csv')

    # blob.upload_from_filename(source_file_name)

    print(blobs)
    )
bucket_name = 'j-first-bucket'
save_path = 'route/'
list_blob('bucket_name')


os.chdir("/Users/jireh.park/jireh_module/svc_data/route")

# 데이터 불러오기
df = pd.DataFrame()
for fl in os.listdir():
    if 'txt' in fl:
        data = pd.read_csv(fl,
                           engine = 'python', encoding = 'cp949', sep = '|', dtype = str)
        df = df.append(data)

df = df.reset_index(drop = True)

# start, destination 뒤집어서 저
size = len(df)
col = ['time', 'num_station', 'transfer']
for ii in tqdm(df.index):
    aa = df.loc[ii, 'route'][2:-2].split("', '")
    aa.reverse()

    df.loc[size + ii, 'start'] = df.loc[ii, 'destination']
    df.loc[size + ii, 'destination'] = df.loc[ii, 'start']
    for cl in col:
        df.loc[size + ii, cl] = df.loc[ii, cl]
        
    df.loc[size + ii, 'route'] = aa

df = df.reset_index(drop = True)
df.to_csv("route.csv", encoding = 'cp949', index = False)
#df.to_json("route.json")
