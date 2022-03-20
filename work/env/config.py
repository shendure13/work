# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 13:30:16 2021

@author: NICE
"""

from configparser import ConfigParser
import os

CONFIG_PATH = os.path.dirname(os.path.abspath(__file__))

def check_section(filename=CONFIG_PATH + '/database.ini'):
    
    config = ConfigParser()
    config.read(filename)
    sec = config.sections()
       
    return sec

def config(filename=CONFIG_PATH + '/database.ini', section=None):
    """
        :type filename: str
        :type section: str
        :return ini_dict: {}
    """
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    ini_dict = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            ini_dict[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
        
    return ini_dict


