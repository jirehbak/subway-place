#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 10:51:20 2020

@author: jireh.park
"""


#% 데이터 불러오기
import os
import sys
import pandas as pd
import numpy as np
import re
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

def data_cleansing():

    path = '../data/'
    sub_index = pd.read_csv(path + "sub_index.txt", encoding = 'cp949', sep = '|', engine = 'python', dtype = str)
    sub_dist = pd.read_csv(path + "sub_dist.txt", encoding = 'cp949', sep = '|', engine = 'python', dtype = str)
    
    sub_hot = pd.read_csv(path + "sub_hot2.txt", encoding = 'cp949', sep = '|', engine = 'python', dtype = str)
    sub_hot2 = pd.read_csv(path + "sub_hot3.txt", encoding = 'cp949', sep = '|', engine = 'python', dtype = str)
    sub_hot3 = pd.read_csv(path + "sub_hot4.txt", encoding = 'cp949', sep = '|', engine = 'python', dtype = str)
    
    sub_hot['역번호'] = sub_hot['역번호'].str.zfill(4)
    sub_hot2['역번호'] = sub_hot2['역번호'].str.zfill(4)
    sub_hot3['역번호'] = sub_hot3['역번호'].str.zfill(4)
    
    sub_dist['transfer'] = '0'
    col = ['transfer', 'statnfnm', 'statntnm', 'sctntime', 'statnf_cd',
           'statnf_line', 'statnt_cd', 'statnt_line']
    
    sub_dist = sub_dist[col]
    
    # 환승 데이터 셋팅
    trans_df = pd.DataFrame(columns = col)
    ind = 10000
    for nm in sub_dist['statntnm'].unique(): #nm = '홍대입구'
        sub_nm = sub_dist[sub_dist['statntnm'] == nm]
        line_list = sub_nm['statnt_line'].unique().tolist()
        trans_list = []
        for l in line_list:
            for l2 in line_list:
                if l == l2:
                    pass
                else:
                    trans_list.append([l, l2])
        for tr in trans_list: #tr = trans_list[0]
            trans_df.loc[ind, 'transfer'] = '1'
            trans_df.loc[ind, 'statnfnm'] = nm
            trans_df.loc[ind, 'statntnm'] = nm
            trans_df.loc[ind, 'sctntime'] = 10
            trans_df.loc[ind, 'statnf_cd'] = sub_index[(sub_index['station_nm'] == nm) & (sub_index['line_num'] == tr[0])]['station_cd'].tolist()[0]
            trans_df.loc[ind, 'statnf_line'] = tr[0]
            trans_df.loc[ind, 'statnt_cd']= sub_index[(sub_index['station_nm'] == nm) & (sub_index['line_num'] == tr[1])]['station_cd'].tolist()[0]
            trans_df.loc[ind, 'statnt_line'] = tr[1]
            ind += 1
    
    sub_dist = sub_dist.append(trans_df)
    sub_dist['sctntime'] = sub_dist['sctntime'].astype(int)
    # message reply function
    #def get_message(bot, update) :
    #    update.message.reply_text("got text")
    #    update.message.reply_text(update.message.text)
    sub_dist = sub_dist[sub_dist['sctntime'] <= 10]
    
    return sub_index, sub_dist
    
