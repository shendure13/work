# -*- coding: utf-8 -*-
"""
Created on Mon Nov 15 11:23:32 2021

@author: NICE
"""

import os, configparser

config = configparser.ConfigParser()

config["postgresql"] = {}
config["postgresql"]["host"] = "localhost"
config["postgresql"]["database"] = "localhost"
config["postgresql"]["user"] = "postgres"
config["postgresql"]["password"] = "postgres"
config["postgresql"]["port"] = "5432"

config["mongdb"] = {}
config["mongdb"]["host"] = "localhost"
config["mongdb"]["port"] = "5432"
config["mongdb"]["database"] = "localhost"
config["mongdb"]["collection"] = ""

with open('C:/Users/NICE/Desktop/bravo/pythonproject/work/env/database.ini', 'w') as configfile:
    config.write(configfile)





