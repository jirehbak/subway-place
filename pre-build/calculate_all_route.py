#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 10:55:25 2020

@author: jireh.park
"""


#path 설정
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

# library
from io import StringIO
import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import datetime

from appointment.data_cleansing import data_cleansing
from appointment.appointment import *
import datalab.storage as gcs
from datalab.context import Context
from google.cloud import storage


def main():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.dirname(
        os.path.abspath(os.path.dirname(__file__))) + '/key/level-district.json'

    sub_index, df = data_cleansing()
    sub_names = df.statnfnm.unique().tolist()
    sub_pairs = get_pairs_from_list(sub_names)

    bucket_name = 'j-first-bucket'
    save_path = 'route/'
    st = datetime.now()
    all_route = pd.DataFrame()
    start2 = 0
    ii = 0
    st2 = '종로3가'

    for pair in sub_pairs:

        start = pair[0]
        destination = pair[1]

        if start != start2:
            if (len(all_route) > 0) & (start2 == st2):
                file_name = 'route_%d_%s_rev.txt' % (ii, start2)
                upload_blob(bucket_name, save_path + file_name, all_route)

            all_route = pd.DataFrame()

        if start != st2:
            continue

        print("%s\t%s\t\t%3.2f\t%s" % (start, destination, round(ii / len(sub_pairs) * 100, 2), str(datetime.now() - st)))
        meta_info = interactive_route(df, sub_index, start, destination)

        all_route.loc[ii, 'start'] = start
        all_route.loc[ii, 'destination'] = destination
        all_route.loc[ii, 'time'] = meta_info[0]
        all_route.loc[ii, 'num_station'] = meta_info[1]
        all_route.loc[ii, 'transfer'] = meta_info[2]
        all_route.loc[ii, 'route'] = str(meta_info[3])
        ii += 1

        start2 = pair[0]


def upload_blob(bucket_name, destination_blob_name, df):
    global credentials
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    #f = StringIO()
    df.to_csv("tmp", encoding = 'utf-8')
    blob.upload_from_filename("tmp", content_type='text/csv')

    # blob.upload_from_filename(source_file_name)

    print(
        "File uploaded to {}.".format(
            destination_blob_name
       )
    )

def dataframe_to_gcs(bucketname, path, dataframe):
    global credentials
    gcs.Bucket(bucketname).item(path).write_to(dataframe.to_csv(),'text/csv')

def get_unique_pairs_from_list(lst): # ab-ba 중복제거
    pairs = []
    lst_len = len(lst)
    for i in range(lst_len):
        if i == (lst_len -1):
            break
    
        subset = lst[i+1:]
        for item in subset:
            pairs.append((lst[i], item))
    
    return pairs

def get_pairs_from_list(lst): # ab-ba 중복제
    pairs = []
    lst_len = len(lst)
    for i in range(lst_len):
        for item in lst:
            if lst[i] != item:
                pairs.append((lst[i], item))
            else:
                continue

    return pairs

if __name__ == '__main__':
    main()
