##encoding=utf8

from __future__ import print_function
from angora.SQLITE.core import MetaData, Sqlite3Engine, Table, Column, DataType, Row, Select

engine = Sqlite3Engine(":memory:")
metadata = MetaData()
datatype = DataType()

pythontype = Table("pythontype", metadata,
    Column("list_type", datatype.pythonlist),
    Column("set_type", datatype.pythonset),
    Column("dict_type", datatype.pythondict),
    )

print(pythontype.create_table_sql())

metadata.create_all(engine)

ins = pythontype.insert()
record = ([1,2,3], {1,2,3}, {1:"a", 2:"b", 3:"c"})

engine.insert_record(ins, record)
 
engine.prt_all(pythontype)