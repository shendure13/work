# -*- coding: utf-8 -*-
"""
Created on Wed Jun 30 17:39:48 2021

@author: e2on
"""

import pandas as pd
import numpy as np
import os, re, math, datetime
# import geopandas, os, re, math, datetime
import psycopg2 as pg
# from shapely.wkb import loads
from datetime import datetime
# from tqdm import tqdm

#%% =============================================================================
#%% Connection
#%% =============================================================================
#%% - connect_postgre

def connect_postgre(db_name):
    
    if db_name == "adt_test":
        con_postgre = pg.connect(host='192.168.0.20', dbname='adt_test', user='adt_test', password='adt_test1!')
    elif db_name == "newdeal":
        con_postgre = pg.connect(host='192.168.0.20', dbname='newdeal', user='newdeal', password='newdeal1!')
    elif db_name == "uninan":
        con_postgre = pg.connect(host='192.168.0.137', dbname='uninan', user='uninan', password='uninan1234')
    elif db_name == "postgres":
        con_postgre = pg.connect(host='localhost', dbname='postgres', user='postgres', password='postgre')

    cur = con_postgre.cursor()
    
    return con_postgre, cur

#%% - get_databases

def get_databases():
    sql = "select distinct datname from pg_database;"

#%% - get_table_list_query

def get_table_list_query(db_name, cur):
    
    con_postgre, cur = connect_postgre(db_name)
    
    # table 리스트 출력
    table_name_query = "select tablename from pg_tables order by tablename asc"
    cur.execute(table_name_query)
    table_name_lst = cur.fetchall()
    table_name_list = []
    for i in table_name_lst:
        table_name_list.append(''.join(i))    
    print(f"테이블 목록 : {table_name_list}")
    
#%% - get_table_query

def get_table_query(cur, table_name):
    
    # 테이블이 속한 스키마
    schema_query = f"select schemaname from pg_tables where tablename='{table_name}'"
    cur.execute(schema_query)
    schema_name = ''.join(cur.fetchall()[0])    # 스키마명 str으로 변환
    table_query = f'select * from {schema_name}.{table_name}'
    columns_query = f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' ORDER BY ORDINAL_POSITION"
    
    return table_query, columns_query

#%% - get_table_query_2

def get_table_query_2(cur, schema_name, table_name):

    table_query = f'select * from {schema_name}.{table_name}'
    columns_query = f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
    
    return table_query, columns_query
    
#%% - get_schema

def get_schema(cur):
    schema_query = f"select distinct schemaname from pg_tables"

#%% - find_column_names

def find_column_names(cur, columns_query):
    # 컬럼명 조회
    cur.execute(columns_query)
    columns = cur.fetchall()
    columns_lst = []
    for i in columns:
        columns_lst.append(''.join(i))
        
    return columns_lst

#%% - execute_table

def execute_table(cur, query):
    
    # 조회
    cur.execute(query)
    results = cur.fetchall()
    
    return results

#%% =============================================================================
#%% Tools
#%% =============================================================================
#%% - change_type

def change_type(df, column, type):
    
    if type == "str":
        df[column] = df[column].astype(str)

    elif type == "int":
        df[column] = df[column].astype(int)
    
    elif type == "float":
        df[column] = df[column].astype(float)

    return df

#%% - decoding_text

def decoding_text(text):
    
    from urllib.parse import unquote
    unicode_text = text
    decode_text = unquote(unicode_text)
    print(decode_text)
    
    return decode_text

#%% - get_avg

def get_avg(x):
    d = {}
    d['avg_sales'] = x['sales'].mean()
    return pd.Series(d, index=['avg_sales'])

#%% - get_condition_info

# df1 = 
# df2 = 

# result_lst = []

# for i in range(len(df1)):
                    
#     dist_info = df1.iloc[i,:]
#         dist_info_column_lst = []
#         for dist_info_column  in dist_info.index:
#             dist_info_column_lst.append(dist_info_column)
#             globals()[dist_info_column] = dist_info[dist_info_column]

#     condition = utility.get_isin(
#         utility.get_isin(
#             utility.get_isin(
#                 df2, "addr1", [addr1]),
#             "year", [year]),
#         "month", [month])
    
#     try:
#         result = dist_info["value"] / condition["value"]
#     except Exception as e:
#         print(e)
#         result = pd.Series([0.0])
    
#     result_lst.append(result[0])

#%% - get_date_and_day

def get_date_and_day(current_time):

    # current_time = datetime.datetime.now()   # 현재 날짜 & 시간
    result_time = datetime.datetime.strftime(current_time, '%Y-%m-%d %a %H:%M:%S')

    return result_time

#%% - get_groupby

def get_groupby(df, group_column): # group_column : list형식
    grouped_df = df.groupby(group_column).apply(get_total).reset_index()
    
    # Null값만 있는 컬럼 삭제
    if len(grouped_df['total_sales']) == grouped_df['total_sales'].isnull().sum():        
        grouped_df.drop("total_sales", axis=1, inplace=True)

    if len(grouped_df['total_count']) == grouped_df['total_count'].isnull().sum():
        grouped_df.drop("total_count", axis=1, inplace=True)

    if len(grouped_df['total_value']) == grouped_df['total_value'].isnull().sum():
        grouped_df.drop("total_value", axis=1, inplace=True)
        
    if len(grouped_df['total_floating_pop']) == grouped_df['total_floating_pop'].isnull().sum():
        grouped_df.drop('total_floating_pop', axis=1, inplace=True)

    return grouped_df


#%% - get_groupby_2

def get_groupby_2(df, group_column): # group_column : list형식

    grouped_df = df.groupby(group_column).apply(get_total_2).reset_index()
    
    # Null값만 있는 컬럼 삭제
    if len(grouped_df[f'total_sales']) == grouped_df['total_sales'].isnull().sum():        
        grouped_df.drop("total_sales", axis=1, inplace=True)

    return grouped_df

#%% - get_info

def get_info(df):
    df.info()
    df.fillna(value=0, inplace=True)
    print(list(df.columns))
    for column in df.columns:
        print(f"\n\n << {column} >> \n {np.sort(df[column].unique())}")

#%% - get_isin

def get_isin(df, column, value, TF=True):    # value : list    

    df_1 = pd.DataFrame(df.values, columns=df.columns)

    if TF == False:
        df_1 = df_1[~df_1[column].isin(value)]
    else:
        df_1 = df_1[df_1[column].isin(value)]
    
    df_1 = get_reset_index(df_1)
    
    return df_1

#%% - get_like

def get_like(df, column, value, TF=True):

    df_1 = pd.DataFrame(df.values, columns=df.columns)

    if TF == False:
        df_1 = df_1[~df_1[column].str.contains(value, case=False, na=False)]
    else:
        df_1 = df_1[df_1[column].str.contains(value, case=False, na=False)]
    
    df_1 = get_reset_index(df_1)
    
    return df_1    


#%% - get_join_dist

def get_join_addr(df):
    
    df_1 = pd.DataFrame(df.values, columns=df.columns)

    df_1["addr1"] = df_1["addr1"] + " " + df_1["addr2"]
    df_1.rename(columns={"addr1":"dist"}, inplace=True)
    df_1.drop(["addr2"], axis=1, inplace=True)    
    
    # 공백 삭제
    dist_lst = []
    for dist in df_1["dist"]:
        dist_lst.append(dist.strip())    
    
    # # 컬럼 순서정렬
    # columns_lst = df_1.columns
    # columns_lst = columns_lst.insert(0, "dist")
    # df_1 = df_1[columns_lst]    
    
    return df_1


#%% - get_location_info

def get_location_info(columns_lst):

    '''
    좌표정보를 갖고 있는 컬럼 선택
    '''

    lon = ""
    lat = ""
    geom = ""
    for column in columns_lst:
        if "lon" in column or "위도" in column:
            lon = column
        elif "lat" in column or "경도" in column:
            lat = column
        elif "geom" in column:
            geom = column
    
    # count_location_info = len(set([ lon, lat, geom ]) & set(columns_lst))
    location_info = set([ lon, lat, geom ]) & set(columns_lst)  # 해당 테이블이 갖고있는 좌표정보 컬럼
    print(f"좌표정보 컬럼 : {location_info}")
    
    return lon, lat, geom, location_info

#%% - get_null

def get_null(df, value):
   
    if value == True:
        null_cnt = pd.DataFrame(df.isnull().sum(), columns=["null_cnt"]).reset_index().rename(columns={"index":"column_nm"})
        return null_cnt
    if value == False:
        not_null_cnt = pd.DataFrame(df.notnull().sum(), columns=["not_null_cnt"]).reset_index().rename(columns={"index":"column_nm"})
        return not_null_cnt

    

#%% - get_quarter_and_month

def get_quarter_and_month(quarter):
    
    if quarter == 1:
        month = [1,2,3]

    elif quarter == 2:
        month = [4,5,6]

    elif quarter == 3:
        month = [7,8,9]

    elif quarter == 4:
        month = [10,11,12]
    
    return month


#%% - get_rate

def get_rate_or_value(cal_way):

    try:
        if cal_way == "/":
            rate_value = dist_info[value_column] / condition[cal_value_column]
            rate_count = dist_info[count_column] / condition[cal_count_column]
        elif cal_way == "*":
            rate_value = dist_info[value_column] * condition[cal_value_column]
            rate_count = dist_info[count_column] * condition[cal_count_column]

    except:
        rate_value= pd.Series([0.0])
        rate_count= pd.Series([0.0])
    
    rate_lst.append(rate_value[0])
    rate_count_lst.append(rate_count[0])
    
    
#%% - get_rate_of_change

def get_rate_of_change(pre_year, cur_year, column_name):        # 증감
    
    rate_of_change_lst = []
    
    for i in range(len(cur_year)):
        ratio = ( (cur_year[column_name].iloc[i] - pre_year[column_name].iloc[i]) / pre_year[column_name].iloc[i] )
        # ratio = round(((cur_year[column_name].iloc[i] - pre_year[column_name].iloc[i]) / pre_year[column_name].iloc[i]), 2)
        rate_of_change_lst.append(ratio)
        
    return rate_of_change_lst

#%% - get_reset_index

def get_reset_index(df):

    df_1 = pd.DataFrame(df.values, columns=df.columns)
    df_1 = df_1.reset_index().drop("index", axis=1)
    
    return df_1


#%% - get_shpfile

def get_shpfile(table_name, columns_lst):
    
    # df = globals()[table_name].sample(frac=0.03, replace=False)
    
    root_path = "C:/Users/e2on/Desktop"
    folder_name = f"{table_name}_shp"
    result_folder = os.mkdir(root_path + "/" + folder_name)
    
    # SHP파일로 저장
    globals()[table_name].to_file(f"{root_path}/{folder_name}/{table_name}_G_001.shp", encoding='utf-8', header=False)
    # globals()[table_name].to_file(f"C:/Users/e2on/Desktop/작업 폴더/{table_name}_G_001.shp", encoding='utf-8', header=False)

    # DB에 load된 데이터와 SHP파일 row_count 확인
    gdf = geopandas.read_file(f"{root_path}/{folder_name}/{table_name}_G_001.shp")
    
    print("="*50)
    print(f"DB row_count : {len(globals()[table_name])}")
    print(f"SHP 파일 row_count : {len(gdf)}")
    print("="*50)
    
    # row_count 같을 시 csv저장
    if len(globals()[table_name]) == len(gdf):
        gdf.to_csv(f"{root_path}/{folder_name}/{table_name}_G_001.csv", encoding='cp949', header=True)


#%% - get_table_info

def get_table_info(table_name):
    print("="*50)
    print(f"Data : {table_name}")
    print(f"{globals()[table_name].info()}")
    print("="*50)

#%% - get_total

def get_total(x):
    d = {}
    try:
       
        if "sales" in x.columns:
            d['total_sales'] = x['sales'].sum()

        if "total_sales" in x.columns:
            d['total_sales'] = x['total_sales'].sum()
        
        if "month_sales" in x.columns:
            d['total_sales'] = x['month_sales'].sum()

        if "sales_count" in x.columns:
            d['total_count'] = x['sales_count'].sum()
            
        if "total_count" in x.columns:
            d['total_count'] = x['total_count'].sum()
        
        if "month_sales_count" in x.columns:
            d['total_count'] = x['month_sales_count'].sum()
            
        if "value" in x.columns:
            d['total_value'] = x['value'].sum()
            
        if "floating_pop" in x.columns:
            d['total_floating_pop'] = x['floating_pop'].sum()
        
        if "total_floating_pop" in x.columns:
            d['total_floating_pop'] = x['total_floating_pop'].sum()
        
        if "floating_data" in x.columns:
            d['total_floating_pop'] = x['floating_data'].sum()
        
        if "floating_pop" in x.columns:
            d['total_floating_pop'] = x['floating_pop'].sum()
            
        if "total_value" in x.columns:
            d['total_value'] = x['total_value'].sum()
            
        else:
            pass
            
    except Exception as e:
        print(e)
        

    return pd.Series(d, index=['total_sales', 'total_count', 'total_value', 'total_floating_pop'])


#%% - get_total_2

def get_total_2(df, column):
    d = {}
    try:       
        if column in df.columns:
            d[f'total_{column}'] = df[column].sum()
            
        else:
            pass
            
    except Exception as e:
        print(e)
        

    return pd.Series(d, index=[f'total_{column}'])


#%% - get_type_info

def get_type_info(df):
    for column in df.columns:
        print(f"{column} == {type(df[column][0])}")

#%% - get_value_type

def get_value_type(df):
    
    print("<< value_type >>")
    print("-"*20)
    for column in df.columns:
        print(f"{column} : {type(column[0])}")


#%% - create_geom

def create_geom(table_name, columns_lst):
    
    lon, lat, geom, location_info = get_location_info(columns_lst)
    geometry = geopandas.points_from_xy(globals()[table_name][lon], globals()[table_name][lat], crs="EPSG:4326")
    globals()[table_name] = geopandas.GeoDataFrame(globals()[table_name], geometry=geometry)
    globals()[table_name].rename(columns={"geometry":"geom"}, inplace=True)
    
    return globals()[table_name]

#%% - convert_datetime_to_str

def convert_datetime_to_str(table_name, columns_lst):
    
    for column in columns_lst:
        # if globals()[table_name].column.dtypes.name == 'datetime64[ns]':
        if isinstance(globals()[table_name][column][0], datetime) == True:
            globals()[table_name][column] = globals()[table_name][column].apply(lambda x: str(x))    
            print(f"{column} 컬럼, 'str'으로 type변환")

#%% - convert_wkb_to_wkt

def convert_wkb_to_wkt(table_name, columns_lst):
    
    lon, lat, geom, location_info = get_location_info(columns_lst)
    
    # wkb → wkt
    
    wkt_values = []
    for wkb in globals()[table_name][geom]:
        try:
            apoint = loads(wkb, hex=True)
            wkt_values.append(apoint.wkt)
        except:
            wkt_values.append(wkb)
    globals()[table_name][geom] = wkt_values
    # globals()[table_name].astype("geometry")
        
    return globals()[table_name]

#%% - copy_df

def copy_df(df):
    
    df_1 = pd.DataFrame(df.values, columns=df.columns)
    
    return df_1

#%% - preprocess_month

def preprocess_month(df):
    
    df_1 = pd.DataFrame(df.values, columns=df.columns)
    
    month_lst = []
    for month in df_1["month"]:
        month = re.sub("^[0]", "", month)
        month_lst.append(month)    
    df_1["month"] = month_lst
    df_1["month"] = df_1["month"].astype(int)
    
    return df_1

#%% - preprocess_yearmonth

def preprocess_yearmonth(df, column):
    
    df_1 = pd.DataFrame(df.values, columns=df.columns)
    
    # yearmonth --> "year" 와 "month"컬럼으로 분리
    month_lst = []
    year_lst = []
    
    if type(df_1[column][0]) == int:
        df_1[column] = df_1[column].astype(str)
    
    for yearmonth in df_1[column]:
        month_lst.append(yearmonth[4:])    # month만 추출하여 month_lst에 추가
        year = re.sub("(\d\d)$", "", yearmonth)    # 202005 -> 2020
        year_lst.append(year)
    
    df_1["month"] = month_lst
    
    df_1.rename(columns={"yearmonth":"year"}, inplace=True)
    df_1["year"] = year_lst
    df_1["year"] = df_1["year"].astype(int)
    
    # month 정제
    month_lst = []
    for month in df_1["month"]:
        month = re.sub("^[0]", "", month)
        month_lst.append(month)    
    df_1["month"] = month_lst
    df_1["month"] = df_1["month"].astype(int)
    
    return df_1

#%% - read_csv

def read_csv(file_name):
    
    file_path = f"C:/Users/e2on/Desktop"
    
    try:
        globals()[file_name] = pd.read_csv(f"{file_path}/{file_name}.csv", encoding='utf-8')
        globals()[file_name].drop('Unnamed: 0', axis=1, inplace=True)
        print("=" * 60)
        print(f'{file_name} / reading_encoding : utf-8')
        print("=" * 60)
        print("\n")
    except:
        globals()[file_name] = pd.read_csv(f"{file_path}/{file_name}.csv", encoding='cp949')
        globals()[file_name].drop('Unnamed: 0', axis=1, inplace=True)
        print("=" * 60)
        print(f'{file_name} / reading_encoding : cp949')
        print("=" * 60)
        print("\n")

#%% - save_csv

def save_csv(df, file_name, encoding_type):
    
    file_path = f"C:/Users/e2on/Desktop"
    
    if encoding_type == "cp949":
        df.to_csv(f"{file_path}/{file_name}.csv", index=False, encoding=encoding_type)
        print("=" * 50)
        print("save_encoding : cp949")
        print("=" * 50)
        
    elif encoding_type == "utf-8":
        df.to_csv(f"{file_path}/{file_name}.csv", index=False, encoding=encoding_type)
        print("=" * 50)
        print("save_encoding : utf-8")
        print("=" * 50)

#%% - select_quarter

# 해당 분기에 해당하는 "month"명, 컬럼명을 list형으로 return

def select_quarter(quarter):
    
    if quarter == 1:
         month_str = ["Jan", "Feb", "Mar"]
         month_int = ["01", "02", "03"]        
         
    elif quarter == 2:
         month_str = ["Apr", "May", "Jun"]
         month_int = ["04", "05", "06"]
        
    elif quarter == 3:
         month_str = ["Jul", "Aug", "Sep"]
         month_int = ["07", "08", "09"]
         
    elif quarter == 4:
         month_str = ["Oct", "Nov", "Dec"]
         month_int = ["10", "11", "12"]
        
    return month_str, month_int

#%% - split_dataset_and_groupby

def split_dataset_and_groupby(df, group_columns): # group_columns : list
        
    n = int(len(df) / 10)    
    
    df_1 = df[:n]
    df_2 = df[n:n*2]
    df_3 = df[n*2:n*3]
    df_4 = df[n*3:n*4]
    df_5 = df[n*4:n*5]
    df_6 = df[n*5:n*6]
    df_7 = df[n*6:n*7]
    df_8 = df[n*7:n*8]
    df_9 = df[n*8:n*9]
    df_10 = df[n*9:]
   
    df_lst = [df_1, df_2, df_3, df_4, df_5, df_6, df_7, df_8, df_9, df_10]
    df_group_lst = ["df_1_group", "df_2_group", "df_3_group", "df_4_group", "df_5_group", "df_6_group", "df_7_group", "df_8_group", "df_9_group", "df_10_group"]
    
    for i,j in zip(df_lst,df_group_lst):
        globals()[j] = get_groupby(i, group_columns)        
        print("success grouping")

    df_grouped = pd.concat([df_1_group, df_2_group, df_3_group, df_4_group, df_5_group, df_6_group, df_7_group, df_8_group, df_9_group, df_10_group], ignore_index=True)
    
    return df_grouped


#%% - split_addr

def split_addr(df, dist_column):
    
    df_1 = pd.DataFrame(df.values, columns=df.columns)
    
    addr1 = []
    addr2 = []
    addr3 = []
    for dist in df_1[dist_column]:
        split_dist = dist.split(" ")
        if len(split_dist) == 1:
            split_dist.append("")
            split_dist.append("")
            addr1.append(split_dist[0])
            addr2.append(split_dist[1])
            addr3.append(split_dist[2])
        elif len(split_dist) == 2:
            split_dist.append("")
            addr1.append(split_dist[0])
            addr2.append(split_dist[1])
            addr3.append(split_dist[2])
        elif len(split_dist) == 3:
            addr1.append(split_dist[0])
            addr2.append(split_dist[1])
            addr3.append(split_dist[2])
    
    df_1["addr1"] = addr1
    df_1["addr2"] = addr2
    df_1["addr3"] = addr3
    del df_1[dist_column]
    # df_1 = df_1[['addr1', 'addr2', 'addr3', 'year', 'quarter', 'total_value']]
    
    return df_1

#%% - split_addr_ver2

def split_addr_ver2(df, addr_column):
    
    df_1 = df.copy()
    
    # sido_lst = []
    # gungu_lst = []
    # dong_lst = []
    
    addr_arr = np.array()
    
    for addr in df_1[addr_column]:
        addr_list = addr.split(" ")
        np.append(addr_list)
        
    
    return df_1

    

#%% - split_sido_and_sigungu

def split_sido_and_sigungu(df):
    
    # 광역자치구 거주인구    
    sido_lst = ["강원도", "경기도", "경상남도", "경상북도", "광주광역시", "대구광역시", "대전광역시", "부산광역시", "서울특별시", "세종특별자치시", "울산광역시", "인천광역시", "전라남도", "전라북도", "제주특별자치도", "충청남도", "충청북도"]
    resident_pop_sido = pd.DataFrame(columns=df.columns)
    for sido in sido_lst:
        sido_data = get_isin(df, "dist", [sido])        
        resident_pop_sido = pd.concat([resident_pop_sido, sido_data], ignore_index=True)
    
    
    # 시군구 거주인구
    sido_lst = ["강원도", "경기도", "경상남도", "경상북도", "광주광역시", "대구광역시", "대전광역시", "부산광역시", "서울특별시", "울산광역시", "인천광역시", "전라남도", "전라북도", "제주특별자치도", "충청남도", "충청북도"]
    resident_pop_sigungu = pd.DataFrame(df.values, columns=df.columns)
    for sido in sido_lst:
        resident_pop_sigungu = resident_pop_sigungu[resident_pop_sigungu["dist"] != sido]
    resident_pop_sigungu = get_reset_index(resident_pop_sigungu)
    
    return resident_pop_sido, resident_pop_sigungu


#%% - split_timestamp

def split_timestamp(df, timestamp_column_nm):
    
    df_1 = pd.DataFrame(df.values, columns=df.columns)
    
    year_lst = []
    month_lst = []
    date_lst = []
    hour_lst = []
    minute_lst = []
    seconds_lst = []
    
    for timestamp in df_1[timestamp_column_nm]:
        
        timestamp_yearmonth = timestamp.split(" ")[0].split("-")
        timestamp_time = timestamp.split(" ")[1].split(":")
        timestamp_split = timestamp_yearmonth + timestamp_time
        year_lst.append(timestamp_split[0])
        month_lst.append(timestamp_split[1])
        date_lst.append(timestamp_split[2])
        hour_lst.append(timestamp_split[3])
        minute_lst.append(timestamp_split[4])
        seconds_lst.append(timestamp_split[5])
            
    df_1["year"] = year_lst
    df_1["month"] = month_lst
    df_1["date"] = date_lst
    df_1["hour"] = hour_lst
    df_1["minute"] = minute_lst
    df_1["seconds"] = seconds_lst
    
    return df_1

#%% - split_year_and_month

def split_year_and_month(df, column):
    
    df_1 = pd.DataFrame(df.values, columns=df.columns)
    
    # yearmonth --> "year" 와 "month"컬럼으로 분리
    month_lst = []
    year_lst = []
    
    if type(df_1[column][0]) == int:
        df_1[column] = df_1[column].astype(str)
    
    for yearmonth in df_1[column]:
        month_lst.append(yearmonth[4:])    # month만 추출하여 month_lst에 추가
        year = re.sub("(\d\d)$", "", yearmonth)    # 202005 -> 2020
        year_lst.append(year)
    
    df_1["month"] = month_lst
    
    df_1.rename(columns={"yearmonth":"year"}, inplace=True)
    df_1["year"] = year_lst
    # df_1["year"] = df_1["year"].astype(str)

    return df_1

#%% - update_sejong

# for dist in df["dist"]:
#     if re.search("세종특별", dist) != None:
#         df["dist"] = df["dist"].replace(dist, "세종특별자치시")

#%% - update_unit_3hours

def update_unit_3hours(df, time_column):
    
    df_1 = pd.DataFrame(df.values, columns=df.columns)
    
    unit_3hours_lst = []
    for hour in df_1[time_column]:
        result = ( int(hour) // 3 ) * 3    # 0 : 0~2시 / 3 : 3~5시 / 6 : 6~8시..
        unit_3hours_lst.append(result)

    df_1[time_column] = unit_3hours_lst
    
    return df_1

#%% - update_unit_quarters

def update_unit_quarters(df, month_column):
    
    df_1 = pd.DataFrame(df.values, columns=df.columns)  # 데이터 덮어씌워지는 것 방지
    
    if type(df_1[month_column][0]) == int:
        df_1[month_column] = df_1[month_column].astype(str)
    
    # month 정제 ( ex. 01월 -> 1월로 변경 )
    month_lst = []
    for month in df_1[month_column]:
        month = re.sub("^[0]+", "", month)
        month_lst.append(month)
    
    df_1[month_column] = month_lst
    df_1[month_column] = df_1[month_column].astype(int)
        
    unit_quarters = []
    for month in df_1[month_column]:
        result = math.ceil( month / 3 )    # 1월 : 1, 2월: 1, 3월 : 1 / 4월 : 2 ...
        unit_quarters.append(result)

    df_1[month_column] = unit_quarters
    df_1.rename(columns={"month":"quarter"}, inplace=True)
    
    return df_1


#%% =============================================================================
#%% Search
#%% =============================================================================

#%% - get_table

def get_table(table_name, columns_lst, results, table_query, con_postgre):
# def get_table(table_name, columns_lst, results):
    
    lon, lat, geom, location_info = get_location_info(columns_lst)
    
    # GeoDataFrame
    if lon in location_info or lat in location_info or geom in location_info:
        globals()[table_name] = get_geo_table(table_name, columns_lst, results, table_query, con_postgre)
        # globals()[table_name] = get_geo_table(table_name, columns_lst, results)

    # DataFrame
    else:
        globals()[table_name] = pd.DataFrame([result for result in results], columns=columns_lst)
    
    return globals()[table_name]

#%% - get_geo_table

def get_geo_table(table_name, columns_lst, results, table_query, con_postgre):
# def get_geo_table(table_name, columns_lst, results):    
    
    lon, lat, geom, location_info = get_location_info(columns_lst)
    
    # geometry 데이터 O
    try:    
        globals()[table_name] = geopandas.GeoDataFrame.from_postgis(table_query, con_postgre)
        
        # 날짜형식 str변경 ( SHP파일로 저장 시 필요 )
        convert_datetime_to_str(table_name, columns_lst)
        
        save_shp = input(f"{table_name}를 GeoDataFrame으로 생성했습니다.. SHP파일로 저장하시겠습니까( y / n ) : ")
        if save_shp == "y":
            get_shpfile(table_name, columns_lst)
        elif save_shp == "n":
            print("SHP파일로 저장하지 않고 종료합니다.")
            
    # geometry 데이터 X
    except:
        globals()[table_name] = geopandas.GeoDataFrame([result for result in results], columns=columns_lst)
        
        # 날짜형식 str변경 ( SHP파일로 저장 시 필요 )
        convert_datetime_to_str(table_name, columns_lst)

        make_geom = input("geom데이터가 없습니다. geom데이터를 생성하시겠습니까( y / n ) : ")
        try:
            if make_geom == "y" and lon in globals()[table_name].columns and lat in globals()[table_name].columns and geom not in globals()[table_name].columns:
            
                create_geom(table_name, columns_lst, lon, lat)
                print("geom데이터를 생성하였습니다.")
                    
                # try:
                #     save_shp = input(f"{table_name}를 GeoDataFrame으로 생성했습니다.. SHP파일로 저장하시겠습니까( y / n ) : ")
                #     if save_shp == "y":
                #         get_shpfile(table_name, columns_lst)
                #     elif save_shp == "n":
                #         print("SHP파일로 저장하지 않고 종료합니다.")
                # except Exception as e:
                #     print(e)
                #     print("저장실패")
                    
            elif make_geom == "n":
                print("geom데이터를 생성하지 않고 종료합니다.")
                
        except:
            print("위경도 데이터가 없습니다.")

    return globals()[table_name]



# def get_table(con_postgre, cur, table_name):
# def get_table(con_postgre, cur):
    
#     con_postgre, cur = postgre_connect_to_adt()
    
#     global schema_name, columns_lst, df
    
    # # table 리스트 출력
    # table_name_query = "select tablename from pg_tables order by tablename asc"
    # cur.execute(table_name_query)
    # table_name_lst = cur.fetchall()
    # table_name_list = []
    # for i in table_name_lst:
    #     table_name_list.append(''.join(i))    
    # print(f"테이블 목록 : {table_name_list}")
    
    # table_name = input("테이블명을 입력하시오 : ")
    
    # # 테이블이 속한 스키마
    # schema_query = f"select schemaname from pg_tables where tablename='{table_name}'"
    # cur.execute(schema_query)
    # schema_name = ''.join(cur.fetchall()[0])    # 스키마명 str으로 변환
    
    # query = f'select * from {schema_name}.{table_name}'
    # columns_query = f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' ORDER BY ORDINAL_POSITION"
    
    # # 조회
    # cur.execute(query)
    # results = cur.fetchall()
    
    # # 컬럼명 조회
    # cur.execute(columns_query)
    # columns = cur.fetchall()
    # columns_lst = []
    # for i in columns:
    #     columns_lst.append(''.join(i))
    
    # 조회할 테이블 (result) /// 테이블명으로 변수명 지정
    # globals()[table_name] = geopandas.GeoDataFrame([result for result in results], columns=columns_lst)
    # globals()[table_name] = pd.DataFrame([result for result in results], columns=columns_lst)
    
    # try:
    #     # geom데이터 수정
    #     if len(globals()[table_name]["geom"]) > 0:
            
    #         # 컬럼에 위도,경도 여부
    #         for column in globals()[table_name].columns:
    #             if "lon" in column:
    #                 lon = column
    #             elif "lat" in column:
    #                 lat = column
            
    #         globals()[table_name] = geopandas.GeoDataFrame(globals()[table_name])
    #         globals()[table_name]["geom"] = None
    #         gdf = globals()[table_name]
    #         geometry = geopandas.points_from_xy(gdf[lon], gdf[lat], crs="EPSG:4326")
    #         gdf = geopandas.GeoDataFrame(gdf, geometry=geometry)
    #         globals()[table_name]["geom"] = gdf["geometry"]
    #         globals()[table_name].drop(["geometry"], axis=1, inplace=True)    
    #         df = globals()[table_name]

    # except:
    #     # geom데이터 없으면 pass
    #     df = globals()[table_name]
    
    # con_postgre.close()
    # cur.close()
    
    # print("="*50)
    # print(f"Data : {table_name}")
    # print(f"{globals()[table_name].info()}")
    # print("="*50)
