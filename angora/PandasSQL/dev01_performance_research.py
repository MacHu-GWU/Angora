##encoding=utf-8

from angora.GADGET.pytimer import Timer
from angora.DATA.timewrapper import timewrapper
from angora.STRING.formatmaster import fmter
from datetime import datetime, date, timedelta
import pandas as pd
import unittest
import sqlite3
import random
import os

timer = Timer()

class PandasReadBigFile(unittest.TestCase):
    """
    验证pandas读取大文件时, 如果只读取前两行, 是否需要的时间极短
    """
#     def setUp(self):
#         df = list()
#         complexity = 10000
#         for _ in range(complexity):
#             df.append( (
#                         fmter.tpl.randstr(32), 
#                         random.randint(1, 1000), 
#                         random.random(),
#                         timewrapper.randdate("1980-01-01", "2015-04-28").\
#                         strftime("%b %m, %Y"),
#                         timewrapper.randdatetime("1980-01-01 00:00:00", "2015-04-28 23:59:59").\
#                         strftime("%m/%d/%Y %I:%M:%S %p")
#                         ) )
#         df = pd.DataFrame(df, columns=["ID", "INTEGER", "REAL", "CREATE_DATE", "CREATE_DATETIME"])
#         df.to_csv(r"testdata\bigcsvfile.txt", index=False)
    
    def test_verify(self):
        """验证读取一个大文件的前几行时间是否小于0.01秒
        """
        timer.start() # 1 - 3 ms
        pd.read_csv(r"testdata\bigcsvfile.txt", nrows=2)
        timer.stop()
        self.assertLess(timer.elapse, 0.01)

    def test_pandas_builtin_datetime_parser(self):
        """验证pandas built-in的date parser的和自定义的timewrapper的性能
        
        注意: 数据库不接受pandas.tslib.timestamp作为时间格式输入
        
        结论: 用timewrapper比较好
            对于标准格式 "2014-01-01" 和 "2014-01-01 18:00:00"来说, pandas比较快。这是因为
            pandas也内置了一系列的日期格式模板, 然后pandas按顺序一个个试验。由于标准模板是
            第一个, 所以速度较快。而比较冷僻的格式, 则每次pandas都需试错多次后才能成功。
            而TimeWrapper能在试验成功后, 将成功的模板作为之后的默认模板。所以免去了许多试错
            的时间。
        """
        timer.start()
        df = pd.read_csv(r"testdata\bigcsvfile.txt", usecols=[3,4], parse_dates=[0,1])
        [i.to_datetime().date() for i in df["CREATE_DATE"]]
        [i.to_datetime() for i in df["CREATE_DATETIME"]]
        timer.timeup()
        
        timer.start()
        df = pd.read_csv(r"testdata\bigcsvfile.txt", usecols=[3,4])
        [timewrapper.str2date(i) for i in df["CREATE_DATE"]]
        [timewrapper.str2datetime(i) for i in df["CREATE_DATETIME"]]
        timer.timeup()
        
#     def tearDown(self):
#         try:
#             os.remove(r"testdata\bigcsvfile.txt")
#         except:
#             pass
    
# unittest.main()

def test_pandas_apply_method_performance():
    """在pandas中apply方法可以用来批量对数据进行修改:
    比如你有一个函数 func(x), 那么你可以用 DataFrame["column_index"].apply(func) 对
    列中所有的数据进行修改。
    那么我们来比较一下apply方法和python的列表推导式的性能。
    
    结论: 差不多没有区别
    """
    df = pd.DataFrame()
    df["a"] = [str(timewrapper.randdatetime("1980-01-01", "2014-01-01")) for i in range(1000)]
    
    timer.start()
    a1 = [timewrapper.str2datetime(i) for i in df["a"]]
    timer.timeup()
    
    timer.start()
    df["a"].apply(timewrapper.str2datetime)
    timer.timeup()

# test_pandas_apply_method_performance()

def test_pandas_timestamp_compatibility_to_database():
    connect = sqlite3.connect(":memory:")
    cursor = connect.cursor()
    cursor.execute("CREATE TABLE test (create_date DATE)")
    
    df = pd.read_csv(r"testdata\bigcsvfile.txt", usecols=["CREATE_DATE"], parse_dates=[0])
    for i in df["CREATE_DATE"]:
        print(i, type(i), i.to_datetime(), type(i.to_datetime().date()))
        cursor.execute("INSERT INTO test VALUES (?);", (i,)) # 数据库不接受pandas.tslib.Timestamp
    for record in cursor.execute("SELECT * FROM test"):
        print(record)

# test_pandas_timestamp_compatibility_to_database()