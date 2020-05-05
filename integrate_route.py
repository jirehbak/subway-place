#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May  1 15:39:06 2020

@author: jireh.park
"""


import pandas as pd
import os
from tqdm import tqdm

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
