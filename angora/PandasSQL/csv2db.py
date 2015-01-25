##encoding=utf8


"""
本脚本用于批量将csv文件存入sqlite3数据库
要求csv文件的格式严格遵守

import:
    from angola.DBA.PSQL.csv2db import PandaSQL
"""

from __future__ import print_function
from sqlalchemy import Table, Column, MetaData
from sqlalchemy import Integer, REAL, TEXT, DateTime, Date
from sqlalchemy import create_engine
from angora.DATA.timewrapper import TimeWrapper
import pandas as pd, numpy as np
import sqlite3
import os

class PandaSQL():
    def __init__(self, database_name):
        self.metadata = MetaData()
        self.engine = create_engine("sqlite:///%s" % database_name, echo=False)
        self.connect = sqlite3.connect(database_name) # 原生sqlite3 API connect
        self.cursor = self.connect.cursor() # 原生sqlite3 API cursor
        self.schema = dict()
        self.tw = TimeWrapper()
    
    def append_file(self, path, table_name=None, sep=",", dtype = dict(), primary_key_name = None):
        """生成数据表的metadata
        dtype支持的数据类型是跟数据库一致的。 有效的数据类型有:
        "TEXT", "INTEGER", "REAL", "DATE", "DATETIME"
        """
        ### Initial table_name
        if not table_name:
            table_name = os.path.splitext(os.path.split(path)[1])[0]
        ### Construct pandas dtype definition and database dtype definition
        _pd_dtype_mapping = {"TEXT": np.str, "INTEGER": np.int64, "REAL": np.float64,
                             "DATE": np.str, "DATETIME": np.str}
        _db_dtype_mapping = {"TEXT": TEXT, "INTEGER": Integer, "REAL": REAL, "DATE": Date, 
                             "DATETIME": DateTime}
        pd_dtype, db_dtype = dict(), dict()
        for column_name, data_type in dtype.items():
            if data_type in _pd_dtype_mapping:
                pd_dtype[column_name] = _pd_dtype_mapping[data_type]
            if data_type in _db_dtype_mapping:
                db_dtype[column_name] = _db_dtype_mapping[data_type]        
        
        ### Read column information from csv
        df = pd.read_csv(path, sep=sep, nrows=1, dtype=pd_dtype)
        
        ### Define the right data type in database for each column
        for name, data_type in zip(df.columns, df.dtypes):
            if name not in db_dtype:
                if data_type in [np.object,]:
                    db_dtype[name] = TEXT
                elif data_type in [np.int64, np.int32, np.int16, np.int8, np.int0, np.int]:
                    db_dtype[name] = Integer
                elif data_type in [np.float64, np.float32, np.float16, np.float]:
                    db_dtype[name] = REAL
        
        ### Construct Database.Table Metadata
        columns = list()
        for name, data_type in zip(df.columns, df.dtypes):
            if name == primary_key_name:
                primary_key_flag = True
            else:
                primary_key_flag = False
            columns.append(Column(name, db_dtype[name], primary_key=primary_key_flag))
        
        _ = Table(table_name, self.metadata, *columns)
        ### Construct Schema metadata
        self.schema[path] = {"table_name": table_name,
                             "pd_dtype": pd_dtype,
                             "db_dtype": db_dtype,
                             "sep": sep}
        
        
    def create_all_table(self):
        self.metadata.create_all(self.engine)

    def insert_all(self, chunk_size = 1000*1000):
        """将定义好的文件中的数据插入到数据库
        采用原生sqlite3 API INSERT
        """
        for path, info in self.schema.items(): # 对于预定义的每一个表
            table = self.metadata.tables[info["table_name"]] # 得到表信息
            insert_cmd ="INSERT INTO %s VALUES (%s);" % (table, # 生成 INSERT SQL 语句
                                                         ",".join(["?"]*len(table.columns)))
            
            print("Push data from %s to table '%s'..." % (path, table)) # 打印辅助信息
            _NUM_OF_DONE = 0
            ### 按照生成器模式读取csv文件, 并存入数据库
            for df in pd.read_csv(path, 
                                  sep=info["sep"],
                                  dtype=info["pd_dtype"], 
                                  iterator=True, 
                                  chunksize=chunk_size):
                
                for column_name, dtype in info["db_dtype"].items(): # 修改Date和DateTime列的dtype
                    if dtype == Date: # 标准化为字符串
                        df[column_name] = df[column_name].apply(self.tw.isodatestr)
                    if dtype == DateTime: # 标准化为字符串
                        df[column_name] = df[column_name].apply(self.tw.isodatetimestr)
                        
                # 将dataframe稳定地插入数据库
                try:
                    self.cursor.executemany(insert_cmd, df.values)
                except:
                    for row in df.values:
                        try:
                            self.cursor.execute(insert_cmd, row)
                        except:
                            pass
                self.connect.commit()
                
                _NUM_OF_DONE += len(df.values) # 打印辅助信息
                print("\t%s of rows have been inserted to table '%s'." % (_NUM_OF_DONE, table))
                
            print("\tFile %s is COMPLETED!" % path) # 打印辅助信息
            
if __name__ == "__main__":
    import time
    
    def test_project():
        """
        1. 连上数据库
        2. 用 PandaSQL.append_file(path, table_name, sep, dtype, primary_key_name) 添加文件的metadata
        3. 用 insert_all(chunk_size) 添加所有数据到数据库
        """
        psql = PandaSQL("test.db")
        psql.append_file(r"test_data\advertisement.txt", 
                         sep=",",
                         dtype={"id": "TEXT", "hour": "DATETIME"}, 
                         primary_key_name="id")
        psql.append_file(r"test_data\employee.txt", 
                         sep=",",
                         dtype={"employee_id": "TEXT", "start_date": "DATE"},
                         primary_key_name="employee_id")
        psql.create_all_table()
        st = time.clock()
        psql.insert_all(chunk_size=1000)
        print(time.clock() - st)

        print(psql.cursor.execute("SELECT COUNT(*) FROM (SELECT * FROM advertisement)").fetchall())
        print(psql.cursor.execute("SELECT COUNT(*) FROM (SELECT * FROM employee)").fetchall())
        try:
            os.remove("test.db")
        except:
            pass
        
    test_project()