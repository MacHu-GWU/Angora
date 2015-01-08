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
This module is re-pack of datetime python standard library
    
    TimeWrapper.str2date(datestr), TimeWrapper.str2datetime(datetimestr)
        parse arbitrary format date string/datetime string to python date/datetime object.
        automatically detect format.
        
    TimeWrapper.day_interval(year, month, day, mode = "str")
    TimeWrapper.month_interval(year, month, mode = "str")
    TimeWrapper.year_interval(year, mode = "str")
        generate day, month, year interval start, end datetime string for SQL BETWEEN query.
    
compatibility: compatible to python2 and python3

prerequisites: None

import:
    from angora.DATA.timewrapper import TimeWrapper
"""

from __future__ import print_function
from datetime import datetime as dt, timedelta as td
import itertools

class TimeWrapper(object):
    """时间包装器 是一个智能处理多种日期, 时间日期格式的转换器
    """
    def __init__(self):
        ## 创建所有的模板
        year_formats = ["%Y", "%y"]
        month_formats = ["%m", "%b", "%B"]
        day_formats = ["%d"]
        pads = ["/", "-", " "]
        date_templates = list()
        
        date_templates.append("%B %d, %Y") # September 20, 2014
        
        for orders in itertools.permutations([year_formats, month_formats, day_formats], 3):
            for od in [(od0, od1, od2) for od0 in orders[0] for od1 in orders[1] for od2 in orders[2] ]:
                for pad in pads:
                    date_templates.append(pad.join(od))
        
        
        self.date_templates = date_templates        # 日期的模板集合
        self.default_date_template = "%Y-%m-%d"     # 日期默认模板
        self.iso_dateformat = "%Y-%m-%d"            # 国际标准模板
        
        self.datetime_templates = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]    # 日期时间的模板集合
        self.default_datetime_templates = "%Y-%m-%d %H:%M:%S"                   # 日期时间默认模板
        self.iso_datetimeformat = "%Y-%m-%d %H:%M:%S"                           # 国际标准模板
        
    ### ====== date manipulate ======
    
    def reformat(self, dtstring, before, after):
        """将dtstring从原来的#before格式转换成#after格式"""
        DT = dt.strptime(dtstring, before)
        return dt.strftime(DT, after)
    
    def str2date(self, datestr):
        """Try strip date string from our 108 date_templates and convert to ISO date format
         If None template matching datestr matching, then raise Error
         
        [Args]
        ------
            datestr: a date str
            
        [Returns]
        ---------
            DATE: a datetime.date object
        
        每次解析时, 先尝试这个默认模板, 如果失败了, 再重新对所有模板进行尝试; 一旦尝试成功, 这将
        当前成功的模板保存为默认模板。
        """
        try:
            return dt.strptime(datestr, self.default_date_template).date()
        except: # 如果默认的模板不匹配, 则重新尝试所有的模板
            pass
        
        for template in self.date_templates: # 对每个template进行尝试, 如果都不成功, 抛出异常
            try:
                DATETIME = dt.strptime(datestr, template) # 如果成功了
                self.default_date_template = template # 保存index到iso_dateformat
                return DATETIME.date()
            except:
                pass
        raise Exception("None template matching '%s'" % datestr)

    def str2datetime(self, datetimestr):
        """Try strip date string from our 2 datetime_templates and convert to ISO datetime format
         If None template matching datetimestr matching, then raise Error
         
        [Args]
        ------
            datetimestr: a datetime str
            
        [Returns]
        ---------
            DATETIME: a datetime.datetime object
        
        每次解析时, 先尝试这个默认模板, 如果失败了, 再重新对所有模板进行尝试; 一旦尝试成功, 这将
        当前成功的模板保存为默认模板。
        """
        try:
            return dt.strptime(datetimestr, self.default_datetime_templates)
        except: # 如果默认的模板不匹配, 则重新尝试所有的模板
            pass
        
        for template in self.datetime_templates: # 对每个template进行尝试, 如果都不成功, 抛出异常
            try:
                DATETIME = dt.strptime(datetimestr, template) # 如果成功了
                self.default_datetime_templates = template # 保存index到iso_dateformat
                return DATETIME
            except:
                pass
        raise Exception("None template matching '%s'" % datetimestr)
    
    """
    在数据库中, 我们经常需要使用:
        SELECT * FROM tablename WHERE create_datetime BETWEEN 'start' and 'end';
    为了方便, 我们提供了day_interval, month_interval, year_interval三个函数能够方便的生成start和end
    日期字符串。例如: month_interval(2014, 3) returns:
        start = "2014-03-01 00:00:00", end = "2014-03-31 23:59:59"
    
    [Notice]
    --------
        生成等间距的datetime序列, 可以使用pandas.date_range函数, 请参考pandas.date_range的部分
    """
    
    @staticmethod
    def day_interval(year, month, day, mode = "str"):
        """ example:
        day_interval(2014, 3, 1, "str") returns: "2014-03-01 00:00:00", "2014-03-01 23:59:59"
        
        str mode return pair of datetime str
        dt mode return pair of datetime object
        """
        start, end = dt(year, month, day), dt(year, month, day) + td(days=1) - td(seconds=1)
        if mode == "dt":
            return start, end
        elif mode == "str":
            return str(start), str(end)
    
    @staticmethod
    def month_interval(year, month, mode = "str"):
        """ example:
        month_interval(2014, 12, "str") returns: "2014-12-01 00:00:00", "2014-12-31 23:59:59"
        
        str mode return pair of datetime str
        dt mode return pair of datetime object
        """
        if month == 12:
            start, end = dt(year, month, 1), dt(year+1, 1, 1) - td(seconds=1)
        else:
            start, end = dt(year, month, 1), dt(year, month+1, 1) - td(seconds=1)
        if mode == "dt":
            return start, end
        elif mode == "str":
            return str(start), str(end)
    
    @staticmethod
    def year_interval(year, mode = "str"):
        """ example:
        year_interval(2014, "str") returns: "2014-01-01 00:00:00", "2014-12-31 23:59:59"
        
        str mode return pair of datetime str
        dt mode return pair of datetime object
        """
        start, end = dt(year, 1, 1), dt(year+1, 1, 1) - td(seconds=1)
        if mode == "dt":
            return start, end
        elif mode == "str":
            return str(start), str(end)

if __name__ == "__main__":
    def UT1():
        """测试 时间区间生成器
        """
        print("{:=^40}".format("UT1"))
        print(TimeWrapper.day_interval(2012, 2, 29, mode = "str") )
        print(TimeWrapper.month_interval(2014, 12, mode = "str") )
        print(TimeWrapper.year_interval(1999, mode = "str") )
        
#     UT1()
    
    def UT2():
        """测试 人工模式 日期格式转换
        """
        print("{:=^40}".format("UT2"))
        timewrapper = TimeWrapper()
        print(timewrapper.reformat("2/21/1998", "%m/%d/%Y", "%Y-%m-%d") )
        try:
            print(timewrapper.reformat("Feb/21/1998", "%m/%d/%Y", "%Y-%m-%d") )
        except Exception as e:
            print(e)
            
#     UT2()

    def UT3():
        """测试 自动模式 日期格式转换
        """
        print("{:=^40}".format("UT3"))
        timewrapper = TimeWrapper()
        
        todo = ["Feb/21/1998", "Dec/12/1998", "08/07/2014",
                "August 15, 2013"]
        for s in todo:
            print("%s parsed as %s" % (s, timewrapper.str2date(s) ) )
        
#     UT3()

    def UT4():
        """测试 自动模式 时间日期格式转换
        """
        print("{:=^40}".format("UT4"))
        timewrapper = TimeWrapper()
        
        todo = ["2014-01-15 06:30:12", "2014-01-20T03:11:43", "2014-03-20 23:47:50Z003"]
        for s in todo:
            print("%s parsed as %s" % (s, timewrapper.str2datetime(s) ) )
            
#     UT4()
