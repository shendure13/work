# -*- coding: utf-8 -*-
"""
Created on Thu Mar 10 11:12:27 2022

@author: NICE
"""

import utility_work
import re
import pandas as pd
import numpy as np

# MongoDB에서 "yogiyo_store" 불러오기

# _business_status == True, 사업자정보 O
# _business_status == False, 사업자정보 X
# _business_status == Null, 사업자정보 미진행
# _modTime, 수집 업데이트 날짜 ( 현시점에서 데이터 확인 다를 경우 있음. )

#%% sido, 결측값 제거
yogiyo_store = yogiyo_store_result

sido_list = list(yogiyo_store["sido"].unique())
print(sido_list)

# sido, sido가 아닌 데이터 제거 ( None, 'None', '요기요시' )
yogiyo_store = utility_work.get_isin(yogiyo_store, "sido", [np.nan], False)
yogiyo_store = utility_work.get_isin(yogiyo_store, "sido", ['None'], False)
yogiyo_store = utility_work.get_isin(yogiyo_store, "sido", ['요기요'], False)

print(list(yogiyo_store["sido"].unique()))

#%% 지역별 가게정보 샘플 추출

district = list(yogiyo_store["sido"].unique())
# district = ['서울', '경기', '인천', '부산', '경남', '대구', '경북', '울산', '전남', '광주', '대전', '충남', '세종', '강원', '충북', '전북', '제주']
# district_column_nm = ["seoul", "gyungi", "incheon", "busan", "kyungnam", "daegu", "kyungbuk", "ulsan", "jeonam", "gwangju", "daejeon", "chungnam", "sejong", "kangwon", "chungbuk", "jeonbuk", "jeju"]

dist_df_sample = []
yogiyo_sample = pd.DataFrame()

# for dist, district_column_nm in zip(district, district_column_nm):
for dist in district:

    yogiyo_store_dist = utility_work.get_isin(yogiyo_store, "sido", [dist])
    
    # 각 지역별 10%의 샘플 추출
    sample_cnt = int(len(yogiyo_store_dist) * 0.1)    
    sample = yogiyo_store_dist.sample(n=sample_cnt).reset_index().drop("index", axis=1)
    
    yogiyo_sample = pd.concat([yogiyo_sample, sample], ignore_index=True)
    print(f"{dist} 샘플생성")

#%% 데이터 지정

# df = yogiyo_store # 전체
# df = yogiyo_sample # 샘플
df = yeong

print(df.columns)

#%% NULL / Not NULL 현황

null_cnt = utility_work.get_null(df, True)   # 컬럼별 NULL 갯수
null_not_cnt = utility_work.get_null(df, False)   # 컬럼별 Not_NULL 갯수

is_null = utility_work.get_isin(null_cnt, "null_cnt", [0], False) # NULL값 있는 컬럼 조회
print(is_null)

#%% 컬럼별 결측값 비율

# NULL 비율
null_cnt["rates"] = round(null_cnt["null_cnt"] / len(df), 2)
print(null_cnt)

# NOT_NULL 비율
null_not_cnt["rates"] = round(null_not_cnt["not_null_cnt"] / len(df), 2)
print(null_not_cnt)



#%% NULL / Not NULL 조회 ( 컬럼 선택 )

column = input("column : ")

globals()[f"{column}_null_data"] = df.loc[df.isnull()[column],:]
globals()[f"{column}_not_null_data"] = df.loc[df.notnull()[column],:]

globals()[f"{column}_result_null"] = utility_work.get_reset_index(globals()[f"{column}_null_data"][["address", "name", "sido", column]])
globals()[f"{column}_result_not_null"] = utility_work.get_reset_index(globals()[f"{column}_not_null_data"][["address", "name", "sido", column]])

print(globals()[f"{column}_result_null"])
print(globals()[f"{column}_result_not_null"])

# {column}의 시도별 NULL 갯수
globals()[f"{column}_null_cnt"] = globals()[f"{column}_result_null"]["sido"].value_counts()


#%% 가게(상가), 지역별 사업자등록번호 수집현황

district = list(df["sido"].unique())

not_crmdata = utility_work.get_reset_index(df.loc[df.isnull()["crmdata"],:])   # 사업자정보 없는 가게 데이터
is_crmdata = utility_work.get_reset_index(df.loc[df.notnull()["crmdata"],:])   # 사업자정보 있는 가게 데이터

# 지역별 상가 총 갯수
total_store_cnt_list = []
for dist in district:
    globals()[dist] = utility_work.get_isin(df, "sido", [dist])
    cnt = len(globals()[dist])
    total_store_cnt_list.append(cnt)
    
# 지역별 상가 사업자번호 미수집 갯수
not_crmdata_cnt_list = []
for dist in district:
    globals()[dist] = utility_work.get_isin(not_crmdata, "sido", [dist])
    cnt = len(globals()[dist])
    not_crmdata_cnt_list.append(cnt)

store_crmdata_cnt = pd.DataFrame()
store_crmdata_cnt["district"] = district
store_crmdata_cnt["total_store_cnt"] = total_store_cnt_list
store_crmdata_cnt["not_crmdata_cnt"] = not_crmdata_cnt_list

# 지역별 사업자등록번호 수집현황 정보
print(store_crmdata_cnt)

store_cnt = store_crmdata_cnt["total_store_cnt"].sum() # 요기요 상가 데이터 갯수
not_crmdata_cnt = store_crmdata_cnt['not_crmdata_cnt'].sum() # 사업자번호 미수집 가게 갯수
rate = f"{int(round( (not_crmdata_cnt / store_cnt), 2 ) * 100)}%" # 비율

print("\n")

print(f"요기요 상가 데이터 갯수 : {store_cnt}")
print(f"요기요 사업자정보 미수집 상가 갯수 : {not_crmdata_cnt} ( {rate} )")


#%% 요기요 프랜차이즈 리스트

# 요기요 전체 상가데이터(yogiyo_store)에서 franchise_name의 Not NULL 조회

fran_nm = []
for name in franchise_name_not_null_data["name"]:
    fran_nm.append(name.split("-")[0])

franchise_name_not_null_data["fran_nm"] = fran_nm
fran_list = franchise_name_not_null_data["fran_nm"].unique()

#%% 지역별, 프랜차이즈별 갯수 현황

# "franchise_name"으로 NOT_NULL값 조회 ( franchise_name_result_not_null )
grouped_fran = franchise_name_result_not_null.groupby(['sido', 'franchise_name']).count().reset_index()[["sido", "franchise_name", "name"]].rename(columns={"name":"count"})


#%% 요기요, 가게(상가) 주소 정제
# sido_list = []
sigungu_list = []
dong_list = []

for i in range(len(yogiyo_store["address"])):
    
    try:
        addr_split = yogiyo_store["address"][i].split(" ")
        # sido = addr_split[0]
        sigungu = addr_split[1]
        dong = addr_split[2]
    except:
        # sido = np.nan
        sigungu = np.nan
        dong = np.nan

    # sido_list.append(sido)
    sigungu_list.append(sigungu)
    dong_list.append(dong)
        
    
    print(f'{yogiyo_store["address"][i]}')

yogiyo_store["sigungu"] = sigungu_list
yogiyo_store["dong"] = dong_list
yogiyo_store["sido_gungu"] = yogiyo_store_addr["sido"] + yogiyo_store_addr["sigungu"]
    

#%% 시도, 시군구, 동별 갯수

sido_cnt = yogiyo_store_addr["sido"].value_counts()
sigungu_cnt = yogiyo_store["sido_gungu"].value_counts()
dong_cnt = yogiyo_store_addr["dong"].value_counts()

#%% 영등포 데이터

yogiyo_store.columns

seoul = utility_work.get_isin(yogiyo_store, "sido", ["서울"])
yeong = utility_work.get_like(seoul, "address", "영등포")

len(yeong["id"].unique())

df_id = pd.DataFrame()
df_id["id"] = yeong["id"].unique()

#%% 사업자명 NONE 값
yeong_result = yeong_result.drop(["Unnamed: 0"], axis=1)

none_company_nm = utility_work.get_isin(yeong_result, "company_name", [np.nan])
none_company_nm["store_nm"].unique()



utility_work.get_isin(yeong_result, "company_name", [np.nan])


#%% 사업자번호 NONE 값


