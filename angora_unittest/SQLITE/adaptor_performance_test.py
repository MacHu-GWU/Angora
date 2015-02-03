##encoding=utf8

"""
将Python中的list或者set存入数据库通常有两种方法
1. 将list/set用pickle.dumps转化成bytestr
2. 将list/set中的元素转化为字符串, 然后用连接符例如"&".join()

本脚本对其读写性能均做了测试, 结论是:
用字符串join的方法写入速度是pickle的两倍, 读取速度是pickle的1.5倍
"""

from __future__ import print_function
from angora.GADGET.pytimer import Timer
from angora.DATA.pk import obj2bytestr, bytestr2obj
from angora.STRING.formatmaster import Template
timer = Timer()
tplt = Template()

complexity = 1000000
python_int_list = [i for i in range(complexity)]
python_str_list = [str(i) for i in range(complexity)]
python_int_set = {i for i in range(complexity)}
python_str_set = {str(i) for i in range(complexity)}

def python_int_list_test():
    """测试用pickle方法和join方法在IO一个INT LIST时的性能
    """
    print(tplt.straightline("python_int_list pickle method"))
    timer.start()
    res = obj2bytestr([str(i) for i in python_int_list])
    timer.timeup()
    
    timer.start()
    res = [int(i) for i in bytestr2obj(res)]
    timer.timeup()
    
    print(tplt.straightline("python_int_list str join method"))
    timer.start()
    res = "&".join([str(i) for i in python_int_list])
    timer.timeup()

    timer.start()
    res = [int(i) for i in res.split("&")]
    timer.timeup()

# python_int_list_test()

def python_str_list_test():
    """测试用pickle方法和join方法在IO一个STR LIST时的性能
    """
    print(tplt.straightline("python_str_list pickle method"))
    timer.start()
    res = obj2bytestr(python_str_list)
    timer.timeup()
    
    timer.start()
    res = bytestr2obj(res)
    timer.timeup()
    
    print(tplt.straightline("python_str_list str join method"))
    timer.start()
    res = "&".join(python_str_list)
    timer.timeup()

    timer.start()
    res = res.split("&")
    timer.timeup()

# python_str_list_test()

def python_int_set_test():
    """测试用pickle方法和join方法在IO一个INT SET时的性能
    """
    print(tplt.straightline("python_int_set pickle method"))
    timer.start()
    res = obj2bytestr({str(i) for i in python_int_set})
    timer.timeup()
    
    timer.start()
    res = [int(i) for i in bytestr2obj(res)]
    timer.timeup()
    
    print(tplt.straightline("python_int_set str join method"))
    timer.start()
    res = "&".join({str(i) for i in python_int_set})
    timer.timeup()

    timer.start()
    res = {int(i) for i in res.split("&")}
    timer.timeup()

# python_int_set_test()

def python_str_set_test():
    """测试用pickle方法和join方法在IO一个STR SET时的性能
    """
    print(tplt.straightline("python_str_set pickle method"))
    timer.start()
    res = obj2bytestr(python_str_set)
    timer.timeup()
    
    timer.start()
    res = bytestr2obj(res)
    timer.timeup()
    
    print(tplt.straightline("python_str_set str join method"))
    timer.start()
    res = "&".join(python_str_set)
    timer.timeup()

    timer.start()
    res = set(res.split("&"))
    timer.timeup()

# python_str_set_test()
