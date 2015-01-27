##encoding=utf8

from __future__ import print_function
from angora.SQLITE.core import MetaData, Sqlite3Engine, Table, Column, DataType, Row, Select
from angora.STRING.formatmaster import FormatMaster
from datetime import date
from pprint import pprint
import os

fm = FormatMaster()

try:
    os.remove("movies.db")
except:
    pass

# engine = Sqlite3Engine("movies.db")
engine = Sqlite3Engine(":memory:")
conn = engine.connect
c = engine.cursor

metadata = MetaData()
datatype = DataType()

col_movie_id = Column("movie_id", datatype.text, primary_key=True)
col_title = Column("title", datatype.text, default="unknown_title")
col_length = Column("length", datatype.integer, default=-1)
col_rate = Column("rate", datatype.real, default=0.01)
col_release_date = Column("release_date", datatype.date, default="0000-01-01")
col_genres = Column("genres", datatype.pickletype, default=set())
movie = Table("movie", metadata, col_movie_id, col_title, col_length, col_rate, col_release_date, col_genres)

metadata.create_all(engine)

def DataType_unittest():
    print(datatype.text, repr(datatype.text))
    print(datatype.integer, repr(datatype.integer))
    print(datatype.real, repr(datatype.real))
    print(datatype.date, repr(datatype.date))
    print(datatype.datetime, repr(datatype.datetime))
    print(datatype.pickletype, repr(datatype.real))
    
# DataType_unittest()

def Column_unittest():
    fm.template._straightline("column repr")
    print("repr(Column) = %s" % repr(col_movie_id))
    print("repr(Column) = %s" % repr(col_title))
    print("repr(Column) = %s" % repr(col_length))
    print("repr(Column) = %s" % repr(col_rate))
    print("repr(Column) = %s" % repr(col_release_date))
    print("repr(Column) = %s" % repr(col_genres))
        
    fm.template._straightline("test column.create_table_sql()")
    print("column.create_table_sql() = %s" % col_movie_id.create_table_sql())
    print("column.create_table_sql() = %s" % col_title.create_table_sql())
    print("column.create_table_sql() = %s" % col_length.create_table_sql())
    print("column.create_table_sql() = %s" % col_rate.create_table_sql())
    print("column.create_table_sql() = %s" % col_release_date.create_table_sql())
    print("column.create_table_sql() = %s" % col_genres.create_table_sql())

    fm.template._straightline("Column.__dict__")
    pprint(col_movie_id.__dict__)

    fm.template._straightline("test column.__operation__(other)")
    print(col_movie_id == "m0001")
    print(col_length > 120)
    print(col_length < 120)
    print(col_rate >= 8.0)
    print(col_rate <= 6.0)
    print(col_release_date <= "2014-01-01")
    print(col_release_date >= "2014-01-01")
    print(col_genres == {"Drama", "Carton"})
    print(col_genres != {"Drama", "Carton"})
    
    print(col_title + "ABCD")
    
# Column_unittest()

def Table_unittest():
    print("str(Table) = %s" % movie)
    print("repr(Table) = %s" % repr(movie))
    print("Table.__dict__ =")
    pprint(movie.__dict__)
    
    fm.template._straightline("test table.column")
    print(movie.movie_id.full_name)
    print(movie.title.full_name)
    print(movie.length.full_name)
    print(movie.rate.full_name)
    print(movie.release_date.full_name)
    print(movie.genres.full_name)
    print(movie.all)
    
    print(movie.create_table_sql())
    
# Table_unittest()

def Row_unittest():
    row = Row( ("movie_id", "title"), ("m0002", "Madagascar 2") )
    print(row)
    print(repr(row))
    
    print(row.movie_id)
    print(row["title"])
    
# Row_unittest()

def Insert_unittest():
    record = ("m0001", "Yes Man!", 95, 6.3, "2010-01-01", {"Drama", "Fantasy"})
    row = Row( ("movie_id", "title"), ("m0002", "Madagascar 2") )
    ins = movie.insert()
    
    ins.sqlcmd_from_record()
    print(ins.insert_sqlcmd)
    
    ins.sqlcmd_from_row(row)
    print(ins.insert_sqlcmd)
    
    print(ins.picklize_record(record))
    print(ins.picklize_row(row))
    
# Insert_unittest()

def Engine_insert_unittest():
    
    def record_generator():
        records = [("m0004", "Yes Man!", 95, "2010-01-01", {"Drama", "Fantasy"}),
                   ("m0005", "Yes Man!", 95, "2010-01-01", {"Drama", "Fantasy"}),]
        for record in records:
            yield record
    
    def row_generator():
        rows = [Row( ("movie_id", "title"), ("m0004", "Madagascar 2") ),
                Row( ("movie_id", "title"), ("m0005", "Madagascar 2") ),]
        for row in rows:
            yield row

    ins = movie.insert()

    def test1():

        record = ("m0001", "Yes Man!", 95, 6.2, "2010-01-01", {"Drama", "Fantasy"})
        engine.insert_record(ins, record)
    
        records = [("m0002", "Yes Man!", 95, 6.2, "2010-01-01", {"Drama", "Fantasy"}),
                   ("m0003", "Yes Man!", 95, 6.2, "2010-01-01", {"Drama", "Fantasy"}),]
        engine.insert_many_records(ins, records)
        
        records = record_generator()
        engine.insert_many_records(ins, records)
        
        engine.prt_all(movie)
        
    test1()
    
    def test2():
        row = Row( ("movie_id", "title"), ("m0001", "Madagascar 2") )
        engine.insert_row(ins, row)
           
        rows = [Row( ("movie_id", "title", "genres"), ("m0002", "Madagascar 2", {"carton"}) ),
                Row( ("movie_id", "title", "genres"), ("m0003", "Madagascar 2", {"carton"}) ),]
        engine.insert_many_rows(ins, rows)
         
        rows = row_generator()
        engine.insert_many_rows(ins, rows)
        
        engine.prt_all(movie)

#     test2()
    
# Engine_insert_unittest()

def Select_unittest():
    records = [("m0001", "The Shawshank Redemption", 142, 9.2, date(1994, 10, 14), {"Drama", "Crime"}),
               ("m0002", "The Godfather", 175, 9.2, date(1972, 3, 24), {"Crime", "Drama"}),
               ("m0003", "The Dark Knight", 152, 8.9, date(2008, 7, 18), {"Action", "Crime", "Drama"}),
               ("m0004", "12 Angry Men", 96, 8.9, date(1957, 4, 11), {"Drama"}),
               ("m0005", "Schindler's List", 195, 8.9, date(1994, 2, 4), {"Biography", "Drama", "History"}),]
    ins = movie.insert()
    engine.insert_many_records(ins, records)
    
    fm.template._straightline("test Select object to SQL command")
    sel = Select(movie.all).where(movie.length > 100, 
                                  movie.release_date < date(2000, 1, 1)).limit(3)
    print(sel)
    
    fm.template._straightline("test Select record")
    for record in engine.select(sel):
        print(record)
    
    fm.template._straightline("test Select row")
    for row in engine.select_row(sel):
        print(row)
        
# Select_unittest()

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
    records = [("m0001", "The Shawshank Redemption", 142, 9.2, date(1994, 10, 14), {"Drama", "Crime"}),
               ("m0002", "The Godfather", 175, 9.2, date(1972, 3, 24), {"Crime", "Drama"}),
               ("m0003", "The Dark Knight", 152, 8.9, date(2008, 7, 18), {"Action", "Crime", "Drama"}),
               ("m0004", "12 Angry Men", 96, 8.9, date(1957, 4, 11), {"Drama"}),
               ("m0005", "Schindler's List", 195, 8.9, date(1994, 2, 4), {"Biography", "Drama", "History"}),]
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
    records = [("m0001", "The Shawshank Redemption", 142, 9.2, date(1994, 10, 14), {"Drama", "Crime"}),
               ("m0002", "The Godfather", 175, 9.2, date(1972, 3, 24), {"Crime", "Drama"}),
               ("m0003", "The Dark Knight", 152, 8.9, date(2008, 7, 18), {"Action", "Crime", "Drama"}),
               ("m0004", "12 Angry Men", 96, 8.9, date(1957, 4, 11), {"Drama"}),
               ("m0005", "Schindler's List", 195, 8.9, date(1994, 2, 4), {"Biography", "Drama", "History"}),]
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
    
Engine_insert_and_update_unittest()


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
