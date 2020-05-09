#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May  1 15:39:06 2020

@author: jireh.park

- google cloud storage 에 저장된 route 파일을 불러와 dataframe으로 통합
- 다시 google storage에 "route_all.txt"라는 이름으로 저장

"""



import pandas as pd
import os
from tqdm import tqdm
from google.cloud import storage

def main():
    # gcs 인증
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))+ '/key/level-district.json'

    # 이름 설정
    bucket_name = 'j-first-bucket'
    save_path = 'route/'
    file_name = 'route_all.txt'

    # blob list
    blobs = list_blob(bucket_name)
    # blobs to dataframe
    data = blobs_to_dataframe(blobs)
    # upload data to gcs
    upload_blob(bucket_name, save_path + file_name, data)


def list_blob(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs())

    return blobs

def blobs_to_dataframe(blobs):
    df = pd.DataFrame()
    for bl in blobs:
        if 'rev' in bl.name:
            with open("tmp.txt", "wb") as file_obj:
                bl.download_to_file(file_obj)
            df = df.append(pd.read_csv("tmp.txt"))
            print(bl.name)
        else:
            pass

    return df


def upload_blob(bucket_name, destination_blob_name, df):
    global credentials
    """Uploads a file to the bucket.
    
    bucket_name = "your-bucket-name"
    destination_blob_name = "storage-object-name"
    df = dataframe to save
    
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    df.to_csv("tmp", encoding = 'utf-8', index = False)
    blob.upload_from_filename("tmp", content_type='text/csv')

    # blob.upload_from_filename(source_file_name)

    print(
        "File uploaded to {}.".format(
            destination_blob_name
        )
    )

if __name__ == '__main__':
    main()
