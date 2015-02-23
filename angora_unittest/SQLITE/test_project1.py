##encoding=utf8

from __future__ import print_function
from angora.SQLITE.core import (MetaData, Sqlite3Engine, Table, Column, DataType, Row, 
                                _and, _or, desc, Select)
from angora.STRING.formatmaster import Template
from datetime import date
from pprint import pprint
import os

tplt = Template()

try:
    os.remove("movies.db")
except:
    pass

# engine = Sqlite3Engine("movies.db")
engine = Sqlite3Engine(":memory:")
metadata = MetaData()
datatype = DataType()
col_movie_id = Column("movie_id", datatype.text, primary_key=True)
col_title = Column("title", datatype.text, default="unknown_title")
col_length = Column("length", datatype.integer, default=-1)
col_rate = Column("rate", datatype.real, default=0.01)
col_release_date = Column("release_date", datatype.date, default="1900-01-01")
col_genres = Column("genres", datatype.pickletype, default=set())
movie = Table("movie", metadata, 
              col_movie_id, 
              col_title, 
              col_length, 
              col_rate, 
              col_release_date, 
              col_genres)
metadata.create_all(engine)

records = [
    ("m0001", "The Shawshank Redemption", 142, 9.2, date(1994, 10, 14), {"Drama", "Crime"}),
    ("m0002", "The Godfather", 175, 9.2, date(1972, 3, 24), {"Crime", "Drama"}),
    ("m0003", "The Dark Knight", 152, 8.9, date(2008, 7, 18), {"Action", "Crime", "Drama"}),
    ("m0004", "12 Angry Men", 96, 8.9, date(1957, 4, 11), {"Drama"}),
    ("m0005", "Schindler's List", 195, 8.9, date(1994, 2, 4), {"Biography", "Drama", "History"}),
    ]

def DataType_unittest():
    """测试DataType类的属性和方法
    name, sqlite_dtype_name, __str__(), __repr__()
    """
    tplt._straightline("DataType unittest")
    
    print(datatype.text.name, datatype.text.sqlite_dtype_name,
          datatype.text, repr(datatype.text))
    
    print(datatype.real.name, datatype.real.sqlite_dtype_name,
          datatype.real, repr(datatype.real))
    
    print(datatype.date.name, datatype.date.sqlite_dtype_name,
          datatype.date, repr(datatype.date))
          
    print(datatype.datetime.name, datatype.datetime.sqlite_dtype_name,
          datatype.datetime, repr(datatype.datetime))
    
    print(datatype.pickletype.name, datatype.pickletype.sqlite_dtype_name,
          datatype.pickletype, repr(datatype.pickletype))
    
    print(datatype.pythonlist.name, datatype.pythonlist.sqlite_dtype_name,
          datatype.pythonlist, repr(datatype.pythonlist))
    
    print(datatype.pythonset.name, datatype.pythonset.sqlite_dtype_name,
          datatype.pythonset, repr(datatype.pythonset))
    
    print(datatype.pythondict.name, datatype.repythondictal.sqlite_dtype_name,
          datatype.pythondict, repr(datatype.pythondict))
    
    print(datatype.strlist.name, datatype.strlist.sqlite_dtype_name,
          datatype.strlist, repr(datatype.strlist))
    
    print(datatype.intlist.name, datatype.intlist.sqlite_dtype_name,
          datatype.intlist, repr(datatype.intlist))
    
    print(datatype.strset.name, datatype.strset.sqlite_dtype_name,
          datatype.strset, repr(datatype.strset))
    
    print(datatype.intset.name, datatype.intset.sqlite_dtype_name,
          datatype.intset, repr(datatype.intset))
    
# DataType_unittest()

def Column_unittest():
    """测试Column类的属性和方法
    """
    tplt._straightline("column repr")
    print("repr(Column) = %s" % repr(col_movie_id))
    print("repr(Column) = %s" % repr(col_title))
    print("repr(Column) = %s" % repr(col_length))
    print("repr(Column) = %s" % repr(col_rate))
    print("repr(Column) = %s" % repr(col_release_date))
    print("repr(Column) = %s" % repr(col_genres))
        
    tplt._straightline("test column.create_table_sql()")
    print("column.create_table_sql() = %s" % col_movie_id.create_table_sql())
    print("column.create_table_sql() = %s" % col_title.create_table_sql())
    print("column.create_table_sql() = %s" % col_length.create_table_sql())
    print("column.create_table_sql() = %s" % col_rate.create_table_sql())
    print("column.create_table_sql() = %s" % col_release_date.create_table_sql())
    print("column.create_table_sql() = %s" % col_genres.create_table_sql())

    tplt._straightline("Column.__dict__")
    pprint(col_movie_id.__dict__)
    pprint(col_title.__dict__)
    pprint(col_length.__dict__)
    pprint(col_rate.__dict__)
    pprint(col_release_date.__dict__)
    pprint(col_genres.__dict__)

    tplt._straightline("test column.__operation__(other)")
    print(col_movie_id == "m0001")
    print(col_length > 120)
    print(col_length < 120)
    print(col_rate >= 8.0)
    print(col_rate <= 6.0)
    print(col_release_date <= "2014-01-01")
    print(col_release_date >= "2014-01-01")
    print(col_genres == {"Drama", "Carton"})
    print(col_genres != {"Drama", "Carton"})
    
# Column_unittest()

def Column_operation_unittest():
    """测试Column的逻辑运算和数值运算
    """
    tplt._straightline("test column.__logic_operation__(other)")
    print((col_movie_id == "m0001").sqlcmd)
    print((col_length > 120).sqlcmd)
    print((col_length < 120).sqlcmd)
    print((col_rate >= 8.0).sqlcmd)
    print((col_rate <= 6.0).sqlcmd)
    print((col_release_date <= "2014-01-01").sqlcmd)
    print((col_release_date >= "2014-01-01").sqlcmd)
    print((col_genres == {"Drama", "Carton"}).sqlcmd)
    print((col_genres != {"Drama", "Carton"}).sqlcmd)
    
    tplt._straightline("test column.__calculating_operation__(other)")
    print((col_movie_id + "abcd").sqlcmd)
    print((col_length - 100).sqlcmd)
    
    a = 6.0 <= col_rate
    b = col_rate <= 8.0
    print(a, b)
    print(a and b)
    
# Column_operation_unittest()

def Table_unittest():
    """测试Table的属性和方法
    """
    print("str(Table) = %s" % movie)
    print("repr(Table) = %s" % repr(movie))
    print("Table.__dict__ =")
    pprint(movie.__dict__)
    
    tplt._straightline("test table.column, table.all")
    print(movie.movie_id.table_name, movie.movie_id.full_name)
    print(movie.title.table_name, movie.title.full_name)
    print(movie.length.table_name, movie.length.full_name)
    print(movie.rate.table_name, movie.rate.full_name)
    print(movie.release_date.table_name, movie.release_date.full_name)
    print(movie.genres.table_name, movie.genres.full_name)
    print(movie.all)
    
    tplt._straightline("test create_table_sql")
    print(movie.create_table_sql())
    
# Table_unittest()

def Row_unittest():
    """测试Row的属性和方法
    """
    row = Row( ("movie_id", "title"), ("m0002", "Madagascar 2") )
    tplt._straightline("__str__ and __repr__ method")
    print(row)
    print(repr(row))
    tplt._straightline("access attribute and item")
    print(row.movie_id)
    print(row["title"])
    
    # change row item
    row["title"] = "good"
    print(row)
    
# Row_unittest()

def Insert_unittest():
    record = ("m0001", "Yes Man!", 95, 6.3, "2010-01-01", {"Drama", "Fantasy"})
    row = Row( ("movie_id", "title", "genres"), ("m0002", "Madagascar 2", {"Anime", "Comedy"}) )
    ins = movie.insert()
    
    tplt._straightline("insert record sqlcmd")
    ins.sqlcmd_from_record()
    print(ins.insert_sqlcmd)
    
    tplt._straightline("insert row sqlcmd")
    ins.sqlcmd_from_row(row)
    print(ins.insert_sqlcmd)
    
    tplt._straightline("record converter")
    print(ins.default_record_converter(record))
    
    tplt._straightline("row converter")
    print(ins.default_row_converter(row))
    
# Insert_unittest()

def Engine_insert_unittest():
    
    def record_generator():
        record_list = [("m0001", "Yes Man!", 95, 6.2, "2010-01-01", {"Drama", "Fantasy"}),
                       ("m0002", "Yes Man!", 95, 6.2, "2010-01-01", {"Drama", "Fantasy"}),]
        for record in record_list:
            yield record
    
    def row_generator():
        row_list = [Row( ("movie_id", "title"), ("m0004", "Madagascar 2") ),
                    Row( ("movie_id", "title"), ("m0005", "Madagascar 2") ),]
        for row in row_list:
            yield row

    ins = movie.insert()

    def test1():
        """测试逐条插入的方法
        """
        record = ("m0001", "Yes Man!", 95, 6.2, "2010-01-01", {"Drama", "Fantasy"})
        row = Row(("movie_id", "title", "length", "rate", "release_date", "genres"),
                  ("m0002", "Yes Man!", 95, 6.2, "2010-01-01", {"Drama", "Fantasy"}))
        
        engine.insert_record(ins, record)
        engine.insert_row(ins,row)
        engine.prt_all(movie)
        
    test1()
    
    def test2():
        """测试批量插入的方法
        """
        engine.insert_many_records(ins, record_generator())
        engine.insert_many_rows(ins, row_generator())
        engine.prt_all(movie)
        
#     test2()
    
# Engine_insert_unittest()

################
#    Select    #
################

def Select_unittest():
    # 测试where中的比较
    sel1 = Select(movie.all).where(movie.length > 100, movie.release_date < date(2000, 1, 1))
    # 测试_and函数
    sel2 = Select(movie.all).where(_and(movie.length > 100, movie.release_date < date(2000, 1, 1)))
    # 测试_or函数
    sel3 = Select(movie.all).where(_or(movie.rate > 6.5, movie.release_date < date(2000, 1, 1)))
    # 测试_and和_or函数结合使用
    sel4 = Select(movie.all).where(
               _or(
                   _and(movie.length > 100, 
                        movie.release_date < date(2000, 1, 1)),
                   movie.rate > 6.5)
               )
    # 测试between方法
    sel5 = Select(movie.all).where(movie.rate.between(5.0, 7.5))
    # 测试like方法
    sel6 = Select(movie.all).where(movie.title.like("%dark%"))
    
    # 测试distinct方法
    sel7 = Select([movie.genres]).distinct()
    # 测试order_by方法
    sel8 = Select(movie.all).order_by("movie_id")
    # 测试limit方法
    sel9 = Select(movie.all).limit(3)
    # 测试offset方法
    sel10 = Select(movie.all).limit(3).offset(2)

    print(sel1.toSQL())
    print(sel2.toSQL())
    print(sel3.toSQL())
    print(sel4.toSQL())
    print(sel5.toSQL())
    print(sel6.toSQL())
    print(sel7.toSQL())
    print(sel8.toSQL())
    print(sel9.toSQL())
    print(sel10.toSQL())
    
# Select_unittest()

def Engine_select_unittest():
    ins = movie.insert()
    engine.insert_many_records(ins, records)

    sel = Select(movie.all).where(_and(movie.length > 100, 
                                       movie.release_date.between(date(1990, 1, 1),
                                                                  date(2000, 1, 1))  ) ).limit(3)
    
    tplt._straightline("test Select record")
    for record in engine.select(sel):
        print(record)
    
    tplt._straightline("test Select row")
    for row in engine.select_row(sel):
        print(row)
        
    tplt._straightline("test Select between")
    for record in engine.select(Select(movie.all).where(movie.rate.between(8.0, 9.9))):
        print(record)

    tplt._straightline("test Select like")
    for record in engine.select(Select(movie.all).where(movie.title.like("%dark%"))):
        print(record)   
        
    tplt._straightline("test Select distinct")
    for record in engine.select(Select([movie.genres]).distinct()):
        print(record)
        
    tplt._straightline("test Select order by")
    for record in engine.select(Select(movie.all).order_by(desc("movie_id")).limit(3)):
        print(record)

    tplt._straightline("test Select limit offset")
    for record in engine.select(Select(movie.all).order_by("release_date").limit(3).offset(1)):
        print(record)    
        
Engine_select_unittest()

def Update_unittest():    
    upd = movie.update().values( title = "ABCDEFG", 
                                 length = movie.length + 9999,
                                 release_date = date(1500, 1, 1),
                                 genres = {"Adventure"}).where(movie.release_date <= date(2000, 1, 1),
                                                               movie.length > 100)
    
    upd.sqlcmd()
    print(upd.update_sqlcmd)

# Update_unittest()

def Engine_update_unittest():
    ins = movie.insert()
    engine.insert_many_records(ins, records)

    upd = movie.update().values( title = "ABCDEFG", 
                                 length = movie.length + 9999,
                                 release_date = date(1500, 1, 1),
                                 genres = {"Adventure"}).where(movie.release_date <= date(2000, 1, 1),
                                                               movie.length > 100)
    engine.update(upd)

    engine.prt_all(movie)

# Engine_update_unittest()

def Engine_insert_and_update_unittest():
    ins = movie.insert()
    engine.insert_many_records(ins, records)
    
    new_records = [("m0001", "The Shawshank Redemption", 999, None, date(1994, 10, 14), {"Drama", "Crime"}),
                   ("m0002", "The Godfather", 999, None, date(1972, 3, 24), {"Crime", "Drama"}),]
    new_rows = [Row(("movie_id", "title", "length", "release_date", "genres"), 
                    ("m0004", "12 Angry Men", 999, date(1957, 4, 11), {"Drama"})),
                Row(("movie_id", "title", "length", "release_date", "genres"), 
                    ("m0005", "Schindler's List", 999, date(1994, 2, 4), {"Biography", "Drama", "History"})),]
    
    engine.insert_and_update_many_records(ins, new_records)
    engine.insert_and_update_many_rows(ins, new_rows)
    engine.prt_all(movie)
    
# Engine_insert_and_update_unittest()

def Metadata_reflect_unittest():
    """get the metadata from existing sqlite database
    """
    engine = Sqlite3Engine("movies.db")
    meta = MetaData()
    meta.reflect(engine)
    
    print(meta.tables["movie"].__dict__)
    for column in meta.tables["movie"].columns.values():
        print(repr(column))
    
# Metadata_reflect_unittest()

def autocommit_unittest():
    engine.autocommit(False)
    records = [("m0001", "The Shawshank Redemption", 142, 9.2, date(1994, 10, 14), {"Drama", "Crime"}),
               ("m0002", "The Godfather", 175, 9.2, date(1972, 3, 24), {"Crime", "Drama"}),
               ("m0003", "The Dark Knight", 152, 8.9, date(2008, 7, 18), {"Action", "Crime", "Drama"}),
               ("m0004", "12 Angry Men", 96, 8.9, date(1957, 4, 11), {"Drama"}),
               ("m0005", "Schindler's List", 195, 8.9, date(1994, 2, 4), {"Biography", "Drama", "History"}),]
    ins = movie.insert()
    engine.insert_many_records(ins, records)
    
    engine.prt_all(movie)
    
# autocommit_unittest()
