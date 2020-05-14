#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  4 15:41:08 2020

@author: jireh.park
"""

# path 설정
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

# library
import pandas as pd
from google.cloud import storage
from appointment.appointment import *
from datetime import datetime

def main():
    global sub_index, hot4, route, route_uni, place_list, bucket_name, save_path, file_name

    # gcs 인증
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.dirname(
        os.path.abspath(os.path.dirname(__file__))) + '/key/level-district.json'

    # 이름 설정
    bucket_name = 'j-first-bucket'
    save_path = 'place/'

    # data setting
    setting_data()
    #print(route_uni)

    # calculate all place and save to gcs
    calculate_place()
    #print(data)



def list_blob(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs())

    return blobs

def blobs_to_dataframe(blobs):
    df = pd.DataFrame()
    for bl in blobs:
        if 'route_all' in bl.name:
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

    df.to_csv("tmp.txt", encoding='utf-8', index=False)
    blob.upload_from_filename("tmp.txt", content_type='text/csv')

    print(
        "File uploaded to {}.".format(
            destination_blob_name
        )
    )

def setting_data():
    global sub_index, hot4, route, route_uni, place_list
    # 역번호 리스트 호출
    path = "../data/"
    sub_index = pd.read_csv(path + "sub_index.txt",
                            encoding = 'cp949', sep = '|', engine = 'python', dtype = str)

    # 핫플레이스 리스트 호출
    hot4 = pd.read_csv(path + "sub_hot4.txt",
                       sep = '|', encoding = 'cp949', dtype = str)

    # route 데이터 호출
    blobs = list_blob(bucket_name)
    route = blobs_to_dataframe(blobs)

    # 중복제거
    route_uni = route.loc[pd.DataFrame(np.sort(route[['start','destination']],1),index=route.index).drop_duplicates(keep='first').index]
    route_uni = route_uni.reset_index(drop = True)

    # 핫플레이스 역명 리스트
    base = basic(df = sub_index)
    place_list = base.code_to_name(hot4['역번호'].tolist())


def calculate_place():
    global sub_index, hot4, route, route_uni, place_list

    # 약속장소 산출
    st = datetime.now()
    s1_ = 0
    place_df = pd.DataFrame()
    for ii in range(len(route_uni)):

        s1 = route_uni.loc[ii, 'start']
        s2 = route_uni.loc[ii, 'destination']
        print("%s\t%s\t\t%3.2f\t%s" %(s1, s2, round(ii / len(route_uni) * 100, 2), str(datetime.now() - st)))

        if s1 != s1_:
            if len(place_df) > 0:
                file_name = "place_%s_%d" %(s1_, ii)
                upload_blob(bucket_name, save_path + file_name, place_df)
            place_df = pd.DataFrame()

        t_df = pd.DataFrame()
        for pl in place_list: #pl = '공덕';
            if (s1 != pl) & (s2 != pl):
                try:
                    r1 = route[(route['start'] == s1) & (route['destination'] == pl)]
                    r2 = route[(route['start'] == s2) & (route['destination'] == pl)]

                    time1 = float(r1['time'].values[0])
                    time2 = float(r2['time'].values[0])

                    t_dif = np.abs(time1-time2)
                    t_sum = np.abs(time1+time2)
                    t_df.loc[pl, '시간_1'] = time1
                    t_df.loc[pl, '시간_2'] = time2
                    t_df.loc[pl, '시간차이'] = t_dif
                    t_df.loc[pl, '시간합'] = t_sum
                    #print(pl + " success")
                except IndexError as e:
                    print(e)
                    continue
                except ValueError as e:
                    print(e)
                    continue
            else:
                #print(pl + " failure_b")
                continue

        t_df['s1'] = s1
        t_df['s2'] = s2
        try:
            a = t_df[t_df['시간차이'] <= 15]
            a = a.sort_values(by = ['시간합'], ascending = True)
            center = a[:5].reset_index(drop = False)
            place_df = place_df.append(center)
        except KeyError as e:
            print(e)
            continue

        s1_ = s1


if __name__ == '__main__':
    main()



    

