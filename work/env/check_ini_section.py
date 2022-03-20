# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 13:39:48 2021

@author: NICE
"""


from configparser import ConfigParser
import os

# filename = 'C:/Users/NICE/Desktop/bravo/pythonproject/work/env'
# filename = 'C:\\Users\\NICE\\Desktop\\bravo\\pythonproject\\work\\env\\database.ini'

filename = 'C:\\Users\\user\\Downloads\\pythonproject (1)\\work\\env'

# create a parser
config = ConfigParser()

# read config file
config.read(filename)

# sec = config.has_section("mongodb") # 해당 section 값 존재 시 True
sec = config.sections() # extract section 

print(sec)


