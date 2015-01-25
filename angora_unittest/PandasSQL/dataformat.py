##encoding=utf8

from __future__ import print_function
from angora.SQLITE.core import MetaData, Sqlite3Engine, Table, Column, DataType, Row, Select
from angora.DATA.timewrapper import TimeWrapper
from collections import deque
import pandas as pd, numpy as np
import sqlite3
import datetime

tw = TimeWrapper()

class CSV():
    def __init__(self, path, 
                 table_name = None, 
                 sep = ",",
                 usecols = None,
                 dtype = dict(), 
                 primary_key_name = None):
        self.path = path
        self.table_name = table_name
        self.sep = sep
        self.usecols = usecols
        self.dtype = dtype
        self.primary_key_name = primary_key_name
    
        self._read_metadata()
        
    def _read_metadata(self):
        self.metadata = MetaData()
        datatype = DataType()
        _pd_dtype_mapping = {"TEXT": np.str, "INTEGER": np.int64, 
                             "REAL": np.float64,
                             "DATE": np.str, "DATETIME": np.str}
        _db_dtype_mapping = {"TEXT": datatype.text, "INTEGER": datatype.integer, 
                             "REAL": datatype.real, 
                             "DATE": datatype.date, "DATETIME": datatype.datetime}

        pd_dtype = dict() # {"column_name": dtype} for part of columns, other columns using default setting
        db_dtype = dict() # {"column_name": dtype} for all columns
        for column_name, data_type in self.dtype.items():
            if data_type in _pd_dtype_mapping:
                pd_dtype[column_name] = _pd_dtype_mapping[data_type]
            if data_type in _db_dtype_mapping:
                db_dtype[column_name] = _db_dtype_mapping[data_type]        
        
        ### Read column information from csv
        if self.usecols:
            df = pd.read_csv(self.path, sep=self.sep, nrows=1, dtype=pd_dtype, usecols=self.usecols)
        else:
            df = pd.read_csv(self.path, sep=self.sep, nrows=1, dtype=pd_dtype)
            
        ### Define the right data type in database for each column
        for column_name, data_type in zip(df.columns, df.dtypes):
            if column_name not in db_dtype:
                if data_type in [np.object,]:
                    db_dtype.setdefault(column_name, datatype.text)
                elif data_type in [np.int64, np.int32, np.int16, np.int8, np.int0, np.int]:
                    db_dtype.setdefault(column_name, datatype.integer)
                elif data_type in [np.float64, np.float32, np.float16, np.float]:
                    db_dtype.setdefault(column_name, datatype.real)
        
        self.pd_dtype = pd_dtype
        self.db_dtype = db_dtype
        
        ### Construct Database.Table Metadata
        columns = list()
        for column_name, data_type in zip(df.columns, df.dtypes):
            if column_name in self.primary_key_name:
                primary_key_flag = True
            else:
                primary_key_flag = False
            columns.append(Column(column_name, db_dtype[column_name], primary_key=primary_key_flag))
        
        Table(self.table_name, self.metadata, *columns)
        self.table = self.metadata.tables[self.table_name]
    
    def generate_records(self, chunksize=1000*1000):
        if self.usecols:
            for df in pd.read_csv(self.path, 
                                  sep=self.sep, 
                                  dtype=self.pd_dtype, 
                                  usecols=self.usecols, 
                                  iterator=True, 
                                  chunksize=chunksize):
                for column_name, dtype in self.db_dtype.items(): # 修改Date和DateTime列的dtype
                    if dtype.name == "DATE": # 标准化为字符串
                        df[column_name] = df[column_name].apply(tw.isodatestr)
                    if dtype.name == "DATETIME": # 标准化为字符串
                        df[column_name] = df[column_name].apply(tw.isodatetimestr)
                        
                for record in df.values:
                    yield record
        else:
            for df in pd.read_csv(self.path, 
                                  sep=self.sep, 
                                  dtype=self.pd_dtype,
                                  iterator=True, 
                                  chunksize=chunksize):
                for column_name, dtype in self.db_dtype.items(): # 修改Date和DateTime列的dtype
                    if dtype.name == "DATE": # 标准化为字符串
                        df[column_name] = df[column_name].apply(tw.isodatestr)
                    if dtype.name == "DATETIME": # 标准化为字符串
                        df[column_name] = df[column_name].apply(tw.isodatetimestr)
                        
                for record in df.values:
                    yield record
                    
class SqliteBlackHole():
    def __init__(self, dbname):
        self.engine = Sqlite3Engine(dbname)
        self.metadata = MetaData()
        self.pipeline = deque()
        
    def add(self, datafile):
        self.pipeline.append(datafile)
        
    def devour_all(self):
        while len(self.pipeline) >= 1:
            print("W")
            datafile = self.pipeline.popleft()
            datafile.metadata.create_all(self.engine)
            
            ins = datafile.table.insert()
            self.engine.insert_many_records(ins, datafile.generate_records())
            pass
            
            
# f = CSV(r"test_data/employee.txt",
#         table_name="employee",
#         sep=",",
#         dtype={"employee_id": "TEXT", "start_date": "DATE"},
#         primary_key_name=["employee_id"])
# 
# for record in f.generate_records():
#     print(record)

if __name__ == "__main__":
    def main():
        bh = SqliteBlackHole("test.db")
        advertisement = CSV(r"test_data/advertisement.txt",
                            table_name="advertisement",
                            sep=",",
                            dtype={"id": "TEXT", "hour": "DATETIME"},
                            primary_key_name=["id"])
        employee = CSV(r"test_data/employee.txt",
                table_name="employee",
                sep=",",
                dtype={"employee_id": "TEXT", "start_date": "DATE"},
                primary_key_name=["employee_id"])

        bh.add(advertisement)
        bh.add(employee)
        bh.devour_all()
        
        bh.engine.prt_howmany(advertisement.table)
        bh.engine.prt_howmany(employee.table)
    main()
        
# conn = sqlite3.connect(":memory:")
# c = conn.cursor()
# c.execute("CREATE TABLE test (aaa INTEGER, bbb TEXT, ccc DATETIME, ddd INTEGER, eee REAL);")
# 
#         
#         
# df = pd.read_csv(r"test_data/employee.txt", parse_dates=[2])    
# print(df.dtypes)
# 
# df["start_date"] = df["start_date"].apply(pd.tslib.Timestamp.to_datetime)
# 
# print(df.dtypes)
# 
# for _, row in df.iterrows():
#     value = row["start_date"]
#     print(value)
#     print(type(value))
#     print(type(value.to_pydatetime()))
#     print(type(value.date()))
#     
#     break
#     
# print(c.execute("SELECT * FROM test").fetchall())


# print(value, value.date(), str(value), type(value))
# if __name__ == "__main__":
#     
#     df = pd.read_csv(r"test_data/employee.txt")
#     print(df)
#     f = CSV(r"test_data/employee.txt", table_name="employee", sep=",", )