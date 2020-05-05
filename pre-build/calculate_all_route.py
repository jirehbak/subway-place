#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 10:55:25 2020

@author: jireh.park
"""


#% 데이터 불러오기
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import datetime

from appointment.data_cleansing import data_cleansing
from appointment.appointment import *
import datalab.storage as gcs

def dataframe_to_gcs:(bucketname, path, dataframe):
    gcs.Bucket(bucketname).item(path).write_to(dataframe.to_csv(),'text/csv')

def get_pairs_from_list(lst):
    pairs = []
    lst_len = len(lst)
    for i in range(lst_len):
        if i == (lst_len -1):
            break
    
        subset = lst[i+1:]
        for item in subset:
            pairs.append((lst[i], item))
    
    return pairs
    
sub_index, df = data_cleansing()
sub_names = df.statnfnm.unique().tolist()
sub_pairs = get_pairs_from_list(sub_names)

# start = '대곡'; destination = '봉명'
# sub_dist = df
# R = route_between_sub(sub_dist, sub_index, start, destination)
# route_df = R.next_stations(R.start_cd_list)
# sub_list = route_df[1].unique().tolist()

# if len(R.start_cd_list) >= 1: # 출발지 입력 제대로 했는지 판별 
#     # 환승 없는 경로
#     R3 = route_between_sub(sub_dist, sub_index, start, destination)
    
#     R3.routes_to_destination_without_transfer(route_df, sub_list)
#     route_list_b = R3.route_list
    
#     # 한번 환승하는 경로 
#     R4 = route_between_sub(sub_dist, sub_index, start, destination)
#     R4.routes_to_destination_with_1_transfer(route_df, sub_list)
#     route_list_c = R4.route_list

#     # 환승 포함 경로        
#     R2 = route_between_sub(sub_dist, sub_index, start, destination)
#     while len(R2.basic.intersect(route_df[R2.step+1], R2.dest_cd_list)) == 0: # 최종 목적지 도달여부 확인
#         route_df, sub_list = R2.expand_route(route_df, sub_list)
#         if R2.step > 100:
#             t+=1
#             break
#     R2.routes_to_destination(route_df, sub_list)
#     route_list_a = R2.route_list
bucket_name = 'j-first-bucket'
save_path = 'route/'
st = datetime.now()
all_route = pd.DataFrame()
start2 = 0
index_st = int(input("index 부터 계산시작: "))
reverse = bool(input("reverse?: "))
start_st = sub_pairs[index_st][0]
ii = index_st

for pair in sub_pairs[index_st:]:
	if reverse:
        start = pair[1]
        destination = pair[0]
    else:
        start = pair[0]
        destination = pair[1]

    if start == start_st:
        continue

    if start != start2:
        if (len(all_route) > 0) & (start != start_st):
            #path = "/Users/jireh.park/jireh_module"
            #all_route.to_csv(path + "/svc_data/route_%d_%s.txt" %(ii, start2),
            #                 encoding = 'cp949', sep = '|', index = False)
            file_name = 'route_%d_%s_rev.txt' %(ii, start2)
            dataframe_to_gcs(bucket_name, save_path + file_name, all_route)
        all_route = pd.DataFrame()
    
    print("%s\t%s\t\t%3.2f\t%s" %(start, destination, round(ii / len(sub_pairs) * 100, 2), str(datetime.now() - st)))
    meta_info = interactive_route(df, sub_index, start, destination)
    
    all_route.loc[ii, 'start'] = start
    all_route.loc[ii, 'destination'] = destination
    all_route.loc[ii, 'time'] = meta_info[0]
    all_route.loc[ii, 'num_station'] = meta_info[1]
    all_route.loc[ii, 'transfer'] = meta_info[2]
    all_route.loc[ii, 'route'] = str(meta_info[3])
    ii += 1
    
    start2 = pair[0]
    # destination2 = pair[1]
    
# all_route


# all_route.to_csv("svc_data/all_route.txt",
#                  encoding = 'cp949', sep = '|', index = False)
