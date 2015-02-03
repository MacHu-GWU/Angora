##encoding=utf8

"""
author: Sanhe Hu

compatibility: python3 ONLY

prerequisites: angora.SQLITE

import:
    from angora.PandasSQL.sqlite3blackhole import Sqlite3BlackHole, CSVFile

"""
from __future__ import print_function
from angora.SQLITE.core import MetaData, Sqlite3Engine, Table, Column, DataType
from angora.DATA.timewrapper import TimeWrapper
from angora.GADGET.logger import Messenger, Log
from collections import deque
import pandas as pd, numpy as np

class CSVFile():
    """a CSV datafile class
    """
    def __init__(self, path, 
                 table_name = None, 
                 sep = ",",
                 usecols = None,
                 dtype = dict(), 
                 primary_key_columns = None):
        self.path = path
        self.table_name = table_name
        self.sep = sep
        self.usecols = usecols
        self.dtype = dtype
        self.primary_key_columns = primary_key_columns
        
        self._read_metadata()
        self.timewrapper = None
        
    def _read_metadata(self):
        """construct the metadata for creating the database table
        """
        self.metadata = MetaData()
        datatype = DataType()
        
        ### map the CSV.dtype definition to pandas.read_csv dtype and sqlite3 dtype
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
        
        ### Read one row, and extract column information from csv
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
            if column_name in self.primary_key_columns:
                primary_key_flag = True
            else:
                primary_key_flag = False
            columns.append(Column(column_name, db_dtype[column_name], primary_key=primary_key_flag))
        
        Table(self.table_name, self.metadata, *columns)
        self.table = self.metadata.tables[self.table_name]
    
    def generate_records(self, chunksize=1000*1000):
        """generator for sqlite3 database friendly record from a data file
        """
        if self.usecols:
            for df in pd.read_csv(self.path, 
                                  sep=self.sep, 
                                  dtype=self.pd_dtype, 
                                  usecols=self.usecols, 
                                  iterator=True, 
                                  chunksize=chunksize):
                for column_name, dtype in self.db_dtype.items(): # 修改Date和DateTime列的dtype
                    if dtype.name == "DATE": # 转换为 datestr
                        df[column_name] = df[column_name].apply(self.timewrapper.isodatestr)
                    if dtype.name == "DATETIME": # 转换为 datetimestr
                        df[column_name] = df[column_name].apply(self.timewrapper.isodatetimestr)
                        
                for record in df.values:
                    yield record
        else:
            for df in pd.read_csv(self.path, 
                                  sep=self.sep, 
                                  dtype=self.pd_dtype,
                                  iterator=True, 
                                  chunksize=chunksize):
                for column_name, dtype in self.db_dtype.items(): # 修改Date和DateTime列的dtype
                    if dtype.name == "DATE": # 转换为 datestr
                        df[column_name] = df[column_name].apply(self.timewrapper.isodatestr)
                    if dtype.name == "DATETIME": # 转换为 datetimestr
                        df[column_name] = df[column_name].apply(self.timewrapper.isodatetimestr)
                
                for record in df.values:
                    yield record
                    
class Sqlite3BlackHole():
    def __init__(self, dbname):
        self.engine = Sqlite3Engine(dbname)
        self.metadata = MetaData()
        self.pipeline = deque()
        self.timewrapper = TimeWrapper()
        self.messenger = Messenger()
        self.log = Log()
        
    def add(self, datafile):
        """add datafile object to data pipeline
        """
        datafile.timewrapper = self.timewrapper
        self.pipeline.append(datafile)
        
    def devour(self):
        """if sqlite3.IntegrityError been raised, skip the record.
        """
        while len(self.pipeline) >= 1:
            self.messenger.show("%s files to process..." % len(self.pipeline))
            datafile = self.pipeline.popleft()
            self.messenger.show("now processing %s..." % datafile.path)
            datafile.metadata.create_all(self.engine)
            
            try:
                ins = datafile.table.insert()
                # insert only, if failed, do nothing
                self.engine.insert_many_records(ins, datafile.generate_records())
                self.messenger.show("\tfinished!")
            except:
                self.log.write(datafile.path)
    
    def update(self):
        """unlike Sqlite3BlackHole.devour(), if sqlite3.IntegrityError been raised, 
        update the record.
        """
        while len(self.pipeline) >= 1:
            self.messenger.show("%s files to process..." % len(self.pipeline))
            datafile = self.pipeline.popleft()
            self.messenger.show("now processing %s..." % datafile.path)
            datafile.metadata.create_all(self.engine)
            
            try:
                ins = datafile.table.insert()
                # insert and update
                self.engine.insert_and_update_many_records(ins, datafile.generate_records())
                self.messenger.show("\tfinished!")
            except:
                self.log.write(datafile.path)

