# -*- coding: utf-8 -*-
"""
Created on Thu Oct 21 11:07:26 2021

@author: e2on
"""

#import psycopg2 as pg
#from pymongo import MongoClient
import pandas as pd
import utility_work, db_conn
from env import config


def connect_psql(db_system):
    ### Server → Database → Schema → Table ###
            
    # Select section ( database.ini )
    sections = config.check_section()
    
    postgresql_sections = []
    for section in sections:
        if "postgresql" in section:
            postgresql_sections.append(section)
    
    print(f"\npostgresql Sections : {postgresql_sections}")
    
    section = postgresql_sections[int(input("Select section (index) : "))]
    
    params = config.config(section=section)
    
    # Connect DB
    connector, cur = db_conn.connect_database(db_system, params)
       
    # Database
    db_lst = db_conn.get_database_list(cur)
    print(f"Databse list : {db_lst}")    
    database = input("Select database : ")
    # database = db_lst[int(input("Select database (index) : "))]      
    db_conn.print_connect(database)

    # Schema
    schema_lst = db_conn.get_schema_list(cur)
    print(f"Schema list : {schema_lst}")
    schema = input("Select schema : ")
    # schema = schema_lst[int(input("Select schema (index) : "))]    
    db_conn.print_connect(schema)

    # Table
    table_lst = db_conn.get_table_list(cur, schema)
    print(f"Table list : {table_lst}")
    table_name = input("Select table : ")
    
    # get_data    
    table_query, columns_query = db_conn.get_table_query(cur, schema, table_name)    
    table_values = db_conn.execute_query(cur, table_query)
    columns = db_conn.find_column_names(cur, columns_query)

    try:
        globals()[table_name] = db_conn.get_postgres(connector, table_query, table_name, table_values, columns)
        print("\n")
        print("="*13)
        print(f"Success Load")
        print("="*13)
        print("\n")
    except:
        print("set_columns")
        table_query, columns_query = db_conn.get_table_query(cur, schema, table_name)    
        table_values = db_conn.execute_query(cur, table_query)
        columns = db_conn.set_columns(cur, columns_query)
        
        globals()[table_name] = db_conn.get_postgres(connector, table_query, table_name, table_values, columns)
        
        print("\n")
        print("="*13)
        print(f"Success Load")
        print("="*13)
        print("\n")

    return globals()[table_name]

def connect_mongo(db_system):
    ### Server → Database → Collection ###
        
    # Select section  ( database.ini )
    sections = config.check_section()
    
    mongodb_sections = []
    for section in sections:
        if "mongodb" in section:
            mongodb_sections.append(section)
    
    print(f"\nmongodb Sections : {mongodb_sections}")
    
    section = mongodb_sections[int(input("Select section (index) : "))]
    
    params = config.config(section=section)
    
    # Connect DB
    client = db_conn.connect_database(db_system, params)
        
    # Database
    database_lst = client.list_database_names()
    print(f"\nDatabse list : {database_lst}")
    database = input("Select database : ")
    db = client.get_database(database)
    
    collection = input("collection : ")
    globals()[collection] = pd.DataFrame(eval(f"db.{collection}.find()"))
    print("Success Load")
    
    # # Collection
    # collection_lst = db.collection_names()
    # print(f"Collections : {collection_lst}")
    # while True:        
    #     collections = input("collection : ").split(",")
        
    #     if collections[0] not in collection_lst:
    #         print(f"Not exist '{collections[0]}'")
    #         continue
    #     else:
    #         break
               
    # for collection in collections:
    
    #     # get_data
    #     globals()[collection] = pd.DataFrame(eval(f"db.{collection}.find()"))
    #     print("Success Load")

    return globals()[collection]

def connect_db():
    #### SELECT DB_Tool
    
    db_tools = ["postgresql", "mongodb"]
    
    print(f"\nDatabase System : {db_tools}")
    
    db_system = db_tools[int(input("Select database system (index) : "))]
    
    db_conn.print_connect(db_system)
    
    #### SELECT Server
    if db_system == "postgresql":        
        return connect_psql(db_system)

    elif db_system == "mongodb":        
        return connect_mongo(db_system)
        

if __name__ == "__main__":
    connect_db()


