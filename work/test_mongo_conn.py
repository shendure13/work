# -*- coding: utf-8 -*-
"""
Created on Wed Mar 16 20:22:58 2022

@author: user
"""


from env import config
import db_conn

sections = config.check_section()

mongodb_sections = []
for section in sections:
    if "mongodb" in section:
        mongodb_sections.append(section)

print(f"\nmongodb Sections : {mongodb_sections}")

section = mongodb_sections[int(input("Select section (index) : "))]

params = config.config(section=section)

client = db_conn.connect_database('mongodb', params)

database_lst = client.list_database_names()
print(f"\nDatabse list : {database_lst}")
database = input("Select database : ")
db = client.get_database(database)
collection_lst = db.collection_names()
