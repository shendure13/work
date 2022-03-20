# -*- coding: utf-8 -*-
"""
Created on Tue Oct 19 10:10:09 2021

@author: e2on
"""

import psycopg2 as pg
from pymongo import MongoClient
import pandas as pd
# import geopandas
import os, re, math, datetime, pprint, inspect
from datetime import datetime
from ast import literal_eval
from env import config

#%% =============================================================================
#%% Connect
#%% =============================================================================
#%% - connect_database

def connect_database(db_system, params):
    
    if db_system == "postgresql":
        
        connector = pg.connect(host=params["host"], dbname=params["database"], user=params["user"], password=params["password"], port=params["port"])

        cur = connector.cursor()
        
        return connector, cur

    elif db_system == "mongodb":        
        
        client = MongoClient(params["host"], int(params["port"]))
    
        return client

#%% =============================================================================
#%% Postgre
#%% =============================================================================
#%% - convert_datetime_to_str

def convert_datetime_to_str(table_name, columns):
    
    for column in columns:
        # if globals()[table_name].column.dtypes.name == 'datetime64[ns]':
        if isinstance(globals()[table_name][column][0], datetime) == True:
            globals()[table_name][column] = globals()[table_name][column].apply(lambda x: str(x))    
            print(f"{column} 컬럼, 'str'으로 type변환")
        
#%% - create_geom

def create_geom(table_name, table_values, columns, lon, lat):
    
    globals()[table_name] = pd.DataFrame([result for result in table_values], columns=columns)
    geometry = geopandas.points_from_xy(globals()[table_name][lon], globals()[table_name][lat], crs="EPSG:4326")
    globals()[table_name] = geopandas.GeoDataFrame(globals()[table_name], geometry=geometry)
    
    return globals()[table_name]

#%% - set_geom
    
def set_geom(connector, table_query, table_name, table_values, columns):
    
    lon, lat, geom, location_info = get_location_info(columns)
    
    # geom 데이터 o ( lat, lon, geom )
    if "geom" in columns or "geometry" in columns:
    # if "lat" in columns and "lon" in columns and "geom" in columns:                
        globals()[table_name] = geopandas.GeoDataFrame.from_postgis(table_query, connector)
        convert_datetime_to_str(table_name, columns)
        
    # geom 데이터 x ( lat, lon )
    elif lat in columns and lon in columns:

        make_geom = input("위경도 데이터가 존재합니다. geom데이터를 생성하시겠습니까( y / n ) : ")       
        
        if make_geom == "y":                
            globals()[table_name] = create_geom(table_name, table_values, columns, lon, lat)
            convert_datetime_to_str(table_name, columns)
            print("geom데이터를 생성하였습니다.")
                
        elif make_geom == "n":
            print("geom데이터를 생성하지 않습니다.")              
            
    return globals()[table_name]

#%% - get_location_info

def get_location_info(columns):

    '''
    좌표정보를 갖고 있는 컬럼 선택
    '''

    lon = ""
    lat = ""
    geom = ""
    for column in columns:
        if "lon" in column or "위도" in column:
            lon = column
        elif "lat" in column or "경도" in column:
            lat = column
        elif "geom" in column:
            geom = column
    
    # count_location_info = len(set([ lon, lat, geom ]) & set(columns_lst))
    location_info = set([ lon, lat, geom ]) & set(columns)  # 해당 테이블이 갖고있는 좌표정보 컬럼
    print(f"좌표정보 컬럼 : {location_info}")
    
    return lon, lat, geom, location_info

#%% - get_table_query

def get_table_query(cur, schema_name, table_name):

    # table_query = f'select * from {schema_name}.{table_name}'
    table_query = f'select * from {schema_name}.{table_name}'
    columns_query = f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' order by ordinal_position;"
    
    return table_query, columns_query

#%% - get_table

def get_table(table_name, columns, table, table_query, con_postgre):
    
    print("-" * 50)
    print(table_name)
    lon, lat, geom, location_info = get_location_info(columns)
    globals()[table_name] = pd.DataFrame([result for result in table], columns=columns)
    
    # geom 데이터 o ( lat, lon, geom )
    if "lat" in columns and "lon" in columns and "geom" in columns:                
        globals()[table_name] = geopandas.GeoDataFrame.from_postgis(table_query, con_postgre)
        convert_datetime_to_str(table_name, columns)
        
    # geom 데이터 x ( lat, lon )
    elif "lat" in columns and "lon" in columns and "geom" not in columns:

        make_geom = input("위경도 컬럼이 존재합니다. geom데이터를 생성하시겠습니까( y / n ) : ")
        try:
            if make_geom == "y" and lon in globals()[table_name].columns and lat in globals()[table_name].columns and geom not in globals()[table_name].columns:

                globals()[table_name] = create_geom(table_name, columns, lon, lat)
                convert_datetime_to_str(table_name, columns)
                print("geom데이터를 생성하였습니다. ( column_name : 'geometry' ) ")
                    
            elif make_geom == "n":
                print("geom데이터를 생성하지 않습니다.")
                
        except:
            print("위경도 데이터가 없습니다.")
          
    return globals()[table_name]

#%% - get_schema_table_list

def get_schema_table_list(cur):
    lst = cur.fetchall()
    lst = re.sub("[\(\)]", "", str(lst))
    lst = literal_eval(re.sub(",,", ",", str(lst)))
    # print(lst)
    return lst

#%% - find_column_names

def find_column_names(cur, columns_query):
    # 컬럼명 조회
    cur.execute(columns_query)
    columns = cur.fetchall()
    columns_lst = []
    for i in columns:
        columns_lst.append(''.join(i))
    
    df_columns = pd.DataFrame(columns_lst)
    columns_lst = list(df_columns.drop_duplicates()[0])
    
    for column in columns_lst:
        column = columns_lst[0]
        if "\ufeff" in column:
            columns_lst.remove(column)
        
    return columns_lst

#%% - execute_query

def execute_query(cur, query):
    
    # 조회
    cur.execute(query)
    result = cur.fetchall()
    
    return result

#%% - get_database

def get_database_list(cur):
    
    db_lst_query = "SELECT datname FROM pg_database;"

    db_lst = execute_query(cur, db_lst_query)
    db_lst = preprocess_list_structure(db_lst)

    return db_lst


#%% - get_schema

def get_schema_list(cur):
    schema_query = f"select distinct schemaname from pg_tables"
    schema_lst = execute_query(cur, schema_query)
    schema_lst = preprocess_list_structure(schema_lst)    
    return schema_lst


#%% - get_table_list

def get_table_list(cur, schema):
    
    tables_query = f"select tablename from pg_tables where schemaname = '{schema}';"
    table_lst = execute_query(cur, tables_query)
    table_lst = preprocess_list_structure(table_lst)

    return table_lst

#%% - preprocess_list_structure

def preprocess_list_structure(lst):
    lst = re.sub("[\(\)]", "", str(lst))
    lst = re.sub("[\,]", "", lst)
    lst = literal_eval(re.sub("(\s)", ", ", lst))
    return lst
    
#%% - get_postgres

def get_postgres(connector, table_query, table_name, table_values, columns):
    
    # GeoDataFrame
    if "lat" in columns and "lon" in columns or "geom" in columns or "geometry" in columns:
        globals()[table_name] = set_geom(connector, table_query, table_name, table_values, columns)
            
    # DataFrame
    else:
        globals()[table_name] = pd.DataFrame([result for result in table_values], columns=columns)
       
    return globals()[table_name]
    

#%% - set_columns

def set_columns(cur, columns_query):
    ## 컬럼명이 중복 X
    if int(len(find_column_names(cur, columns_query))) == int(len(set(find_column_names(cur, columns_query)))):
        columns = find_column_names(cur, columns_query)
    
    ## 컬럼명이 중복조회
    else:
        length = int(len(find_column_names(cur, columns_query)) / 2)    
        columns = find_column_names(cur, columns_query)[0:length]
    
    return columns

#%% - print_connect

def print_connect(db_system=None, server=None, database=None, schema=None):
    if [i for i in retrieve_name(db_system) if i == "db_system"][0] == 'db_system':
        print(f"\n=======================================\nYou select database system '{db_system}'\n=======================================\n")
    elif [i for i in retrieve_name(server) if i == "server"][0] == 'server':
        print(f"\n====================================\nConnect server '{server}'\n====================================\n")
    elif [i for i in retrieve_name(database) if i == "database"][0] == 'database':
        print(f"\n====================================\nConnect database '{database}'\n====================================\n")
    elif [i for i in retrieve_name(schema) if i == "schema"][0] == 'schema':
        print(f"\n====================================\nConnect schema '{schema}'\n====================================\n")


#%% - retrieve_name

# 변수명 → str
def retrieve_name(var):
    
    callers_local_vars = inspect.currentframe().f_back.f_locals.items()
    
    return [var_name for var_name, var_val in callers_local_vars if var_val is var]

# for i in retrieve_name(db_system):
#     if i == "db_system":
#         print(i)

# [i for i in retrieve_name(db_system) if i == "db_system"][0]



