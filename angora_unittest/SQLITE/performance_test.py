##encoding=utf8

from angora.SQLITE.core import MetaData, Sqlite3Engine, Table, Column, DataType, Row, Select
from angora.SQLITE.wrapper import iterC
from angora.STRING.formatmaster import FormatMaster
from angora.GADGET.pytimer import Timer
import random
import os

fm = FormatMaster()
try:
    os.remove("performance.db")
except:
    pass

# engine = Sqlite3Engine("performance.db")
engine = Sqlite3Engine(":memory:")
conn = engine.connect
c = engine.cursor

metadata = MetaData()
datatype = DataType()
col_ID = Column("ID", datatype.integer, primary_key=True)
col_name = Column("name", datatype.text)
test = Table("test", metadata, col_ID, col_name)
metadata.create_all(engine)
ins = test.insert()

records = [(i, "abcdefghijklmnopqrstuvwxyz") for i in range(1000)]
records = records + [(random.randint(1, 1000), "abcdefghijklmnopqrstuvwxyz") for i in range(10)]
rows = [Row(("ID", "name"), (i, "abcdefghijklmnopqrstuvwxyz")) for i in range(1000)]
rows = rows + [Row(("ID", "name"), (random.randint(1, 1000), "abcdefghijklmnopqrstuvwxyz")) for i in range(10)]

timer = Timer()

def insert_test1(): # 4.0 - 5.0 second
    """test insert record one by one 
    """
    timer.start()
    for record in records:
        try:
            engine.insert_record(ins, record)
        except:
            pass
    timer.timeup()
    
    engine.prt_howmany(test)
    
# insert_test1()

def insert_test2(): # 0.01 - 0.03 second
    """test bulk insert record
    """
    timer.start()
    engine.insert_many_records(ins, records)
    timer.timeup()
    
    engine.prt_howmany(test)
    
# insert_test2()

def insert_test3(): # 4.0 - 5.0 second
    """test insert row one by one 
    """
    timer.start()
    for row in rows:
        try:
            engine.insert_row(ins, row)
        except:
            pass
    timer.timeup()
    engine.prt_howmany(test)
    
# insert_test3()

def insert_test4(): # 0.01 - 0.03 second
    """test bulk insert row
    """
    timer.start()
    engine.insert_many_rows(ins, rows)
    timer.timeup()
    engine.prt_howmany(test)
    
# insert_test4()

def insert_and_update_test(): # 0.78 - 0.9 seconds
    """test insert and update
    """
    engine.insert_many_records(ins, records)
    # create some new data
    new_records = [(i, "1234") for i in range(100)] + [(i, "6789") for i in range(900, 1000)]
    timer.start()
    engine.insert_and_update_many_records(ins, new_records)
    timer.timeup()
#     engine.prt_all(test)
    
# insert_and_update_test()

def select_test1():
    """
    """
    engine.insert_many_records(ins, records)
    
    sel = Select(test.all)
#     sel = Select(test.all).limit(10)
#     sel = Select(test.all).where(test.ID >= 500)
    
    timer.start()
    for record in engine.select(sel):
        pass
    timer.timeup()

    timer.start()
    for row in engine.select_row(sel):
        pass
    timer.timeup()

select_test1()

def update_test1():
    engine.insert_many_records(ins, records)
    
    upd = test.update()
    upd = test.update().values(name = "1234").where(test.ID >= 500)
    timer.start()
    engine.update(upd)
    timer.timeup()
    
    engine.prt_all(test)
    
# update_test1()
    