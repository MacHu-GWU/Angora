##################################
#encoding=utf8                   #
#version =py27, py33             #
#author  =sanhe                  #
#date    =2015-01-01             #
#                                #
#    (\ (\                       #
#    ( -.-)o    I am a Rabbit!   #
#    o_(")(")                    #
#                                #
##################################

from __future__ import print_function

# from .BOT import *
#  
# from .DATA import *
#  
# try:
#     from .DATASCI import *
# except Exception as e:
#     print(e)
#      
#  
# from .DBA import *
#      
# try:
#     from .GADGET import *
# except Exception as e:
#     print(e)
#  
# try:
#     from .GEO import *
# except Exception as e:
#     print(e)
#  
# from .LIBRARIAN import *
#  
# from .LINEARSPIDER import *
#
# from .STRING import *

    
# __all__ = ["BOT", "DATA", "DATASCI", "DBA", "GADGET", "GEO", "LIBRARIAN", "LINEARSPIDER", "STRING"]

def help():
    sub_packages = [
    "from angora.BOT imoprt *",
    "from angora.DATA import *",
    "from angora.DATASCI import *",
    "from angora.GADGET import *",
    "from angora.LIBRARIAN import *",
    "from angora.LINEARSPIDER import *",
    "from angora.PandasSQL import *",
    "from angora.SQLITE import *",
    "from angora.STRING import *",
    "from angora.TALA import *",
    ]
    print("{:=^100}".format("sub packages in angora"))
    print("\n".join(sub_packages))
    
def import_help():
    BOT_modules = [
    "from angora.BOT.macro import Bot"
    ]
    print("\n{:=^100}".format("angora.BOT"))
    print("\n".join(BOT_modules))
    
    DATA_modules = [
    "from angora.DATA.dicttree import DictTree",
    "from angora.DATA.dtype import OrderedSet, StrSet, IntSet, StrList, IntList",
    "from angora.DATA.hashutil import md5_str, md5_obj, md5_file, hash_obj",
    "from angora.DATA.invertindex import invertindex",
    """from angora.DATA.iterable import (flatten, flatten_all, nth, shuffled, grouper, grouper_dict, grouper_list,
    running_windows, cycle_running_windows, cycle_slice, count_generator)""",
    "from angora.DATA.js import load_js, dump_js, safe_dump_js, prt_js, js2str",
    "from angora.DATA.pk import load_pk, dump_pk, safe_dump_pk, obj2bytestr, bytestr2obj, obj2str, str2obj",
    "from angora.DATA.timewrapper import TimeWrapper",
    ]
    print("\n{:=^100}".format("angora.DATA"))
    print("\n".join(DATA_modules))
    
    SQLITE_modules = [
    """from angora.SQLITE.core import (MetaData, Sqlite3Engine, Table, Column, DataType, Row, 
    _and, _or, desc, Select)"""                  
    ]
    print("\n{:=^100}".format("angora.SQLITE"))
    print("\n".join(SQLITE_modules))
    
    GADGET_modules = [
    "from angora.GADGET.logger import Messenger, Log",
    "from angora.GADGET.logicflow import tryit",
    "from angora.GADGET.pytimer import Timer"
    ]
    print("\n{:=^100}".format("angora.GADGET"))
    print("\n".join(GADGET_modules))
    
    LIBRARIAN_modules = [
    "from .angora.LIBRARIAN.windowsexplorer import WinFile, WinDir, FileCollections, WinExplorer, string_SizeInBytes",
    "from .angora.LIBRARIAN.winzip import zip_a_folder, zip_everything_in_a_folder",
    ]
    print("\n{:=^100}".format("angora.LIBRARIAN"))
    print("\n".join(LIBRARIAN_modules))
    
    LINEARSPIDER_modules = [
    "from angora.LINEARSPIDER.crawler import Crawler, ProxyManager",
    "from angora.LINEARSPIDER.scheduler import Scheduler",
    ]
    print("\n{:=^100}".format("angora.LINEARSPIDER"))
    print("\n".join(LINEARSPIDER_modules))
    
    PandasSQL_modules = [
    "from .sqlite3blackhole import Sqlite3BlackHole, CSVFile",
    ]
    print("\n{:=^100}".format("angora.PandasSQL"))
    print("\n".join(PandasSQL_modules))
    
    STRING_modules = [
    "from angora.STRING.formatmaster import FormatMaster, Template, Converter",
    "from angora.STRING.stringmatch import StrMatcher",
    ]
    print("\n{:=^100}".format("angora.STRING"))
    print("\n".join(STRING_modules))
    
    TALA_modules = [
    "from angora.TALA.tala import FieldType, Field, Schema, SearchEngine",
    ]
    print("\n{:=^100}".format("angora.TALA"))
    print("\n".join(TALA_modules))
    

    

    



