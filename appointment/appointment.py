#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 10:24:54 2020

@author: jireh.park
"""

# 대곡 봉명
import pandas as pd
import numpy as np
import re


#%%
class basic(): 
    def __init__(self, df):
        self.df = df


    #% 지하철 경로 함수 정의
    def intersect(self, a, b):
        return list(set(a) & set(b))
    
    def union(self, a, b):
        return list(set(a) | set(b))
    
    def a_not_in_b(self, a, b):
        for i in self.intersect(a , b):
            a.remove(i)

    def transfer(self, code):
        if type(code)==list:
            pass
        else:
            code = [code]
        trans_list = []
        for i in code:
            nm = self.df[self.df['station_cd']== i]['station_nm'].unique()[0]
            trns = self.df[self.df['station_nm']== nm]['station_cd'].unique().tolist()
            if len(trns) > 1:
                for j in trns:
                    if i != j:
                        trans_list.append([i, j])
                    else:
                        pass
            else:
                pass
        return trans_list
    
    def code_to_name(self, code):
        if type(code)==list:
            pass
        else:
            code = [code]
        return self.df[self.df['station_cd'].isin(code)]['station_nm'].unique().tolist()
    
    def extract_station(self, text):
#    stn = re.compile('\w+')
        stn = re.compile('[ㄱ-ㅎ가-힣]+')
        st1 = stn.findall(text)[0]
        st2 = stn.findall(text)[1]
        return st1, st2
    
    
class route_between_sub: # df = sub_dist; start = '합정'; destination = '삼성'
    def __init__(self, df, sub_index, start, destination):
        self.df = df
        self.sub_index = sub_index
        self.start = start
        self.destination = destination
        
        self.start_cd_list = self.df[self.df['statnfnm'] == self.start]['statnf_cd'].unique()
        self.dest_cd_list = self.df[self.df['statntnm'] == self.destination]['statnt_cd'].unique().tolist()
        
        self.start_line_list = self.df[self.df['statnfnm'] == self.start]['statnf_line'].unique()
        self.dest_line_list = self.df[self.df['statntnm'] == self.destination]['statnt_line'].unique()
        self.step = 0
        
        self.route_list = []
        self.basic = basic(self.df)

    def common_lines(self):
        return self.basic.intersect(self.start_line_list, self.dest_line_list)
	

    def union_lines(self):
        return self.basic.union(self.start_line_list, self.dest_line_list)
 
        
    def next_stations(self, sub_list):
        couple = self.df[self.df['statnf_cd'].isin(sub_list)][['statnf_cd', 'statnt_cd']]
        couple.columns = [self.step, self.step+1]
        return couple
    
    def expand_route(self, route_df, sub_list):
        self.step += 1
        route_df = pd.merge(route_df, self.next_stations(sub_list), 
                            how = 'left', on = self.step)
        route_df = route_df.dropna()
        sub_list = self.df[self.df['statnf_cd'].isin(sub_list)]['statnt_cd'].unique().tolist()
        
        # 중복 제거
        for ss in range(2):
            self.basic.a_not_in_b(sub_list, route_df[self.step - ss])
        
        return route_df, sub_list
    
    def extract_code_with_samelines(self, code_list,line_list):
        stations = self.sub_index[self.sub_index['station_cd'].isin(code_list)]
        stations_in_lines = stations[stations['line_num'].isin(line_list)]
        return stations_in_lines['station_cd'].tolist()
    
    def expand_route_to_commonline(self, route_df, sub_list):
    
        self.step += 1
        
        next_stnt = self.next_stations(sub_list)
        next_stations_in_lines = self.extract_code_with_samelines(next_stnt[self.step+1], self.common_lines())
        couple_in_lines = next_stnt[next_stnt[self.step+1].isin(next_stations_in_lines)]
        
        route_df = pd.merge(route_df, couple_in_lines, 
                            how = 'left', on = self.step)
        route_df = route_df.dropna()
        sub_list = self.df[self.df['statnf_cd'].isin(sub_list)]['statnt_cd'].unique().tolist()
        
        # 중복 제거
        for ss in range(2): 
            self.basic.a_not_in_b(sub_list, route_df[self.step - ss])
        
        return route_df, sub_list


    def expand_route_to_union_line(self, route_df, sub_list):
    
        self.step += 1
        
        next_stnt = self.next_stations(sub_list)
        next_stations_in_lines = self.extract_code_with_samelines(next_stnt[self.step+1], self.union_lines())
        couple_in_lines = next_stnt[next_stnt[self.step+1].isin(next_stations_in_lines)]
        
        route_df = pd.merge(route_df, couple_in_lines, 
                            how = 'left', on = self.step)
        route_df = route_df.dropna()
        sub_list = self.df[self.df['statnf_cd'].isin(sub_list)]['statnt_cd'].unique().tolist()
        
        # 중복 제거
        for ss in range(2):
            self.basic.a_not_in_b(sub_list, route_df[self.step - ss])
        
        return route_df, sub_list
    
    def extract_route_for_destination(self, route_df):

        #필요한 경로만 추출
        route_df = route_df.reset_index(drop = True)
        route_a = route_df[route_df[self.step+1].isin(self.dest_cd_list)]
        for jj in route_a.index:
            route_a = route_df.loc[jj].tolist()
            self.route_list.append(route_a)

    def routes_to_destination_without_transfer(self, route_df, sub_list):
        self.step = 0
        t = 0
        if len(self.common_lines()) > 0:
            while len(self.route_list) <= 1:
                if t >= 1:
                    break
                else:
                   if len(self.route_list) == 0:
                       while len(self.basic.intersect(route_df[self.step+1], self.dest_cd_list)) == 0: # 최종 목적지 도달여부 확인
                           route_df, sub_list = self.expand_route_to_commonline(route_df, sub_list)
                           if self.step > 70:
                               t+= 1
                               break
                       if t == 0:       
                        #필요한 경로만 추출
                            self.extract_route_for_destination(route_df)
                       else:
                            pass
                           
                   else:
                       route_df, sub_list = self.expand_route_to_commonline(route_df, sub_list)
                       
                       while len(self.basic.intersect(route_df[self.step+1], self.dest_cd_list)) == 0:
                           route_df, sub_list = self.expand_route_to_commonline(route_df, sub_list)
                           if self.step > 70:
                               t+= 1
                               break
                       if t == 0:    
                           #필요한 경로만 추출
                           self.extract_route_for_destination(route_df)
                       else:
                           pass
        else:
            pass

    def routes_to_destination_with_1_transfer(self, route_df, sub_list):
        self.step = 0
        t = 0
        while len(self.route_list) <= 1:
            if t >= 1:
                break
            else:
               if len(self.route_list) == 0:
                   while len(self.basic.intersect(route_df[self.step+1], self.dest_cd_list)) == 0: # 최종 목적지 도달여부 확인
                       route_df, sub_list = self.expand_route_to_union_line(route_df, sub_list)
                       if self.step > 70:
                           t += 1
                           break
                   #필요한 경로만 추출
                   if t == 0:
                       self.extract_route_for_destination(route_df)
                   else:
                       pass
                       
               else:
                   route_df, sub_list = self.expand_route_to_union_line(route_df, sub_list)
                   
                   while len(self.basic.intersect(route_df[self.step+1], self.dest_cd_list)) == 0:
                       route_df, sub_list = self.expand_route_to_union_line(route_df, sub_list)
                       if self.step > 70:
                           t += 1
                           break
                   #필요한 경로만 추출
                   if t == 0:
                       self.extract_route_for_destination(route_df)
                   else:
                       pass
           
        
    def routes_to_destination(self, route_df, sub_list):
        self.step = 0
        t = 0
        while len(self.route_list) <= 5:
            if t>= 1:
                break
            else:
               if len(self.route_list) == 0:
                   while len(self.basic.intersect(route_df[self.step+1], self.dest_cd_list)) == 0: # 최종 목적지 도달여부 확인
                       route_df, sub_list = self.expand_route(route_df, sub_list)
                       if self.step > 50:
                           t+=1
                           break
                   if t == 0:        
                       #필요한 경로만 추출
                       self.extract_route_for_destination(route_df)
                   else:
                       pass
                       
               else:
                   route_df, sub_list = self.expand_route(route_df, sub_list)
                   
                   while len(self.basic.intersect(route_df[self.step+1], self.dest_cd_list)) == 0:
                       route_df, sub_list = self.expand_route(route_df, sub_list)
                       if self.step > 50:
                           t += 1
                           break
                   if t== 0:
                       #필요한 경로만 추출
                       self.extract_route_for_destination(route_df)
                   else:
                       pass
                   
#start = '삼성'; destination = '성수'
def calculate_route_between_sub(df, sub_index, start, destination):

    sub_dist = df
    R = route_between_sub(sub_dist, sub_index, start, destination)
    
    if len(R.start_cd_list) >= 1: # 출발지 입력 제대로 했는지 판별 
        route_df = R.next_stations(R.start_cd_list)
        sub_list = route_df[1].unique().tolist()
    
        # 환승 없는 경로
        R3 = route_between_sub(sub_dist, sub_index, start, destination)
        R3.routes_to_destination_without_transfer(route_df, sub_list)
        route_list_b = R3.route_list
        
        # 한번 환승하는 경로 
        R4 = route_between_sub(sub_dist, sub_index, start, destination)
        R4.routes_to_destination_with_1_transfer(route_df, sub_list)
        route_list_c = R4.route_list

        # 환승 포함 경로        
        R2 = route_between_sub(sub_dist, sub_index, start, destination)
        R2.routes_to_destination(route_df, sub_list)
        route_list_a = R2.route_list
        
        for jj in route_list_b:
            route_list_a.append(jj)

        for jj in route_list_c:
            route_list_a.append(jj)

        route_list = route_list_a
        rst_route = []
        rst = pd.DataFrame()
        # route_list 중 최적 경로 선택
        try:
            # 각 경로별로 dataframe 생성
            for ind in range(len(route_list)): # ind = 1
                route_b = route_list[ind]
                df_route = pd.DataFrame(columns = sub_dist.columns)
                for k in range(len(route_b)-1):
                    st = route_b[k]
                    d = route_b[k+1]
                    rt = sub_dist[(sub_dist['statnt_cd'] == d) & (sub_dist['statnf_cd'] == st)]
                    if len(rt) == 1:
                        pass
                    elif len(rt) > 1:
                        rt = rt[rt['statnf_cd'].isin(df_route['statnt_cd'])==False]
                    df_route = df_route.append(rt)
                    
                # 중복 제거
                df_route = df_route.reset_index(drop = True)
                df_route = df_route.sort_values(by = 'sctntime', ascending = True)
                df_route = df_route.drop_duplicates(['statnfnm', 'statntnm'], keep = 'first').reset_index(drop = False)
                df_route = df_route.sort_values(by = 'index', ascending = True).drop('index', axis = 1)
                rst_route.append(df_route)
                
                # rst
                num_station = len(route_b)
                num_transfer = len(df_route['statnt_line'].unique())-1
                time = df_route['sctntime'].astype(int).sum()
                time_tot = time
                
                rst.loc[ind, '정류장수'] = num_station
                rst.loc[ind, '환승횟수'] = num_transfer
                rst.loc[ind, '총 소요시간'] = time_tot
            
        except (IndexError):
            pass
    #            print(step)
        try:
            least_time = rst.loc[rst.sort_values('총 소요시간', ascending = True)[:1].index]
            least_transfer = rst.loc[rst.sort_values(['환승횟수', '총 소요시간'], ascending = [True, True])[:1].index]
            
            least_time_ = rst_route[rst.sort_values('총 소요시간', ascending = True)[:1].index[0]]
            least_transfer_ = rst_route[rst.sort_values(['환승횟수', '총 소요시간'], ascending = [True, True])[:1].index[0]]
            
            rst = least_time.append(least_transfer)
            
            rst_route = []
            rst_route.append(least_time_)
            rst_route.append(least_transfer_)
        
            return rst, rst_route
        except:
            return "입력 오류", "입력 오류"
    else:
        print("출발지 입력 오류")
        return "입력 오류", "입력 오류"


def interactive_route(df, sub_index, x,y):
#    x = input("출발: ")
#    y = input("도착: ")
    j, k = calculate_route_between_sub(df,sub_index, x, y)
    if type(j) != str:
        try:
            t = j['총 소요시간'].min()
            k[0] = k[0][k[0]['transfer'] == '0']
            stnt = len(k[0]) + 1
            trans = j['환승횟수'].min()
            route = k[0]['statnfnm'].tolist() + [y]
    #        route.reverse()
            #    print("총 소요시간: %d분" %(t))
            #    print("환승 횟수: %d회" %(trans))
            #    print("경로: %s" %(route))
            return t, stnt, trans, route
        except KeyError:
            return "입력 오류", "입력 오류", "입력 오류", "입력 오류"
    else:
        return "입력 오류", "입력 오류", "입력 오류", "입력 오류"



#% 중간 장소 산출 함수
def center_point(df, sub_index, s1, s2, dest_list):
    t_df = pd.DataFrame()
    it = 0
    for dest_ in dest_list:
        it += 1
        print(it, dest_)
        rst1, route1 = calculate_route_between_sub(df, sub_index, start = s1, destination = dest_)
        time1 = rst1['총 소요시간'].min()
        num_transfer1 = len(route1[0]['statnt_line'].unique())-1

        rst2, route2 = calculate_route_between_sub(df, sub_index, start = s2, destination = dest_)
        time2 = rst2['총 소요시간'].min()
        num_transfer2 = len(route2[0]['statnt_line'].unique())-1
            
        t_dif = np.abs(time1-time2)
        t_sum = np.abs(time1+time2)
        
        t_df.loc[dest_, '시간_' + s1] = time1
        t_df.loc[dest_, '시간_' + s2] = time2
        t_df.loc[dest_, '시간차이'] = t_dif
        t_df.loc[dest_, '시간합'] = t_sum
    
    a = t_df[t_df['시간차이'] <= 10]
    a = a.sort_values(by = ['시간합'], ascending = True)
    center = a[:5].reset_index(drop = False)
    return center
