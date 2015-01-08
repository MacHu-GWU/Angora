##################################
#encoding=utf8                   #
#version =py27, py33             #
#author  =sanhe                  #
#date    =2014-10-29             #
#                                #
#    (\ (\                       #
#    ( -.-)o    I am a Rabbit!   #
#    o_(")(")                    #
#                                #
##################################

"""
[En]If you run this file as the main script.
    Then package "HSH" will be installed for all Python version you have installed
[Cn]将本脚本作为主脚本运行，会把本脚本所在的package安装到所有用户已安装的python版本的
    site-packages下。不支持需要C预编译文件的库。
"""

from __future__ import print_function
try:
    from .scheduler import Scheduler
    from .crawler import Crawler, ProxyManager
    from angora.DATA.js import load_js, dump_js, safe_dump_js, js2str, prt_js
    from angora.DATA.pk import load_pk, dump_pk, safe_dump_pk, obj2str, str2obj
except:
    pass
