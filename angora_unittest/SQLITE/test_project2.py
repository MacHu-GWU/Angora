##encoding=utf8

from __future__ import print_function
from angora.SQLITE.core import MetaData, Sqlite3Engine, Table, Column, DataType, Row, Select
from angora.DATA.dtype import StrSet, IntSet, StrList, IntList
from collections import OrderedDict
import datetime

class MyClass():
    """a user customized class stored with document
    """
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "MyClass(%s)" % repr(self.value)


engine = Sqlite3Engine(":memory:")
metadata = MetaData()
datatype = DataType()

pythontype = Table("pythontype", metadata,
    Column("uuid", datatype.integer, primary_key=True),
    Column("date_type", datatype.date, default=datetime.date(1999,1,1)),
    Column("datetime_type", datatype.datetime, default=datetime.datetime(2000,1,1,0,0,0)),
    Column("list_type", datatype.pythonlist, default=list()),
    Column("set_type", datatype.pythonset, default=set()),
    Column("dict_type", datatype.pythondict, default=dict()),
    Column("ordereddict_type", datatype.ordereddict, default=OrderedDict()),
    Column("strset_type", datatype.strset, default=StrSet(["a", "b", "c"])),
    Column("intset_type", datatype.intset, default=IntSet([1, 2, 3])),
    Column("strlist_type", datatype.strlist, default=StrList(["x", "y", "z"])),
    Column("intlist_type", datatype.intlist, default=IntList([8, 9, 10])),
    Column("pickletype_type", datatype.pickletype, default={1:"a", 2:"b", 3:"c"}),
    )

print(pythontype.create_table_sql())
 
metadata.create_all(engine)
 
ins = pythontype.insert()

# record = (1, datetime.date(1999,1,1), datetime.datetime(2000,1,1,0,30,45), 
#           [1,2,3], {1,2,3}, {1:"a", 2:"b", 3: "c"}, OrderedDict({1:"a", 2:"b", 3: "c"}),
#           StrSet(["a", "b", "c"]), IntSet([1, 2, 3]), StrList(["x", "y", "z"]), IntList([9, 8, 7]),
#           MyClass(1000))
# engine.insert_record(ins, record)

row = Row(["uuid"], [1])
engine.insert_row(ins, row)

for record in engine.execute("SELECT * FROM pythontype"):
    print(record)
for record in engine.select(Select(pythontype.all)):
    print(record)
engine.prt_all(pythontype)
