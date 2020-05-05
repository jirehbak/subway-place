#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  4 15:41:08 2020

@author: jireh.park
"""


import pandas as pd
import os
from tqdm import tqdm

os.chdir("/Users/jireh.park/jireh_module/appointment/")
from appointment.appointment import *


# 역번호 리스트 호출
path = "/Users/jireh.park/Desktop/appointment/"
sub_index = pd.read_csv(path + "sub_index.txt", 
                        encoding = 'cp949', sep = '|', engine = 'python', dtype = str)

# 핫플레이스 리스트 호출
hot2 = pd.read_csv(path + "sub_hot2.txt",
                   sep = '|', encoding = 'cp949', dtype = str)

hot3 = pd.read_csv(path + "sub_hot3.txt",
                   sep = '|', encoding = 'cp949', dtype = str)

hot4 = pd.read_csv(path + "sub_hot4.txt",
                   sep = '|', encoding = 'cp949', dtype = str)

# route 데이터 호출
path2 = "/Users/jireh.park/jireh_module/svc_data/route/"
route = pd.read_csv(path2 + "route.csv",
                    encoding = 'cp949')
# 중복제거
route_uni = route.loc[pd.DataFrame(np.sort(route[['start','destination']],1),index=route.index).drop_duplicates(keep='first').index]

s1 = '응암' ; s2 = '삼성';
# 핫플레이스 역명 리스트
base = basic(df = sub_index)
place_list = base.code_to_name(hot4['역번호'].tolist())



# 약속장소 산출
place_df = pd.DataFrame()
for ii in tqdm(range(len(route_uni))):
    s1 = route_uni.loc[ii, 'start']
    s2 = route_uni.loc[ii, 'destination']
    print("%s\t%s" %(s1, s2))
    
    t_df = pd.DataFrame()
    for pl in place_list: #pl = '공덕';
        if (s1 != pl) & (s2 != pl):
            
            r1 = route[(route.start == s1) & (route.destination == pl)]
            r2 = route[(route.start == s2) & (route.destination == pl)]
            
            time1 = float(r1.time.values[0])
            time2 = float(r2.time.values[0])
            
            t_dif = np.abs(time1-time2)
            t_sum = np.abs(time1+time2)
            t_df.loc[pl, '시간_1'] = time1
            t_df.loc[pl, '시간_2'] = time2
            t_df.loc[pl, '시간차이'] = t_dif
            t_df.loc[pl, '시간합'] = t_sum
        else:
            continue
        
    t_df['s1'] = s1
    t_df['s2'] = s2
    a = t_df[t_df['시간차이'] <= 10]
    a = a.sort_values(by = ['시간합'], ascending = True)
    center = a[:5].reset_index(drop = False)
    place_df = place_df.append(center)


    
    
    

