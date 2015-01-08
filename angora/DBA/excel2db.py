##################################
#encoding=utf8                   #
#version =py27, py33             #
#author  =sanhe                  #
#date    =2014-10-31             #
#                                #
#    (\ (\                       #
#    ( -.-)o    I am a Rabbit!   #
#    o_(")(")                    #
#                                #
##################################

"""
本脚本用于从excel文件生成一个sqlite database
    每个excel spreadsheet是一个tabel
    每个spreadsheet的column是一个table的column

引擎会按照下表进行数据格式的转换
    int to int, float to float, str to str
    int like str to int, float like str to float
    datetime to datetime, date to datetime
    
compatibility: compatible to python2 and python3

prerequisites: sqlalchemy, pandas

import:
    from angora.DBA.excel2db import excel2sqlite
"""

from __future__ import print_function
from angora.LIBRARIAN.windowsexplorer import WinFile
from sqlalchemy import create_engine
import pandas as pd
import os

def excel2sqlite(excel_name):
    """创建一个与excel文件同名的
    只支持xlsx2013以上的版本
    """
    winfile = WinFile(excel_name)
    engine = create_engine(r"sqlite:///%s.db" % os.path.join(winfile.dirname, 
                                                             winfile.fname), echo = False)
    xls = pd.ExcelFile(excel_name)
    for sheet_name in xls.sheet_names:
        df = xls.parse(sheet_name)
        df.to_sql(con=engine, name=sheet_name, if_exists='replace', flavor='sqlite', index = False)