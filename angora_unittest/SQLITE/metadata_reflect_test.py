##encoding=utf8

from angora.SQLITE import *
from angora.DATA import *
from collections import OrderedDict
import datetime
datatype = DataType()
metadata = MetaData()
engine = Sqlite3Engine(":memory:")

pythontype = Table("pythontype", metadata,
    Column("uuid", datatype.integer, primary_key=True),
    Column("real_type", datatype.real, default=0.001),
    Column("text_type", datatype.text, default="ABCD"),
    Column("date_type", datatype.date, default=datetime.date(2000,1,1)),
    Column("datetime_type", datatype.datetime, default=datetime.datetime.now()),
    Column("list_type", datatype.pythonlist, default=list(), nullable=False),
    Column("set_type", datatype.pythonset, default=set(), nullable=False),
    Column("dict_type", datatype.pythondict, default=dict(), nullable=False),
    Column("ordereddict_type", datatype.ordereddict, default=OrderedDict(), nullable=False),
    Column("strset_type", datatype.strset, default=StrSet(["a", "b", "c"])),
    Column("intset_type", datatype.intset, default=IntSet([1, 2, 3])),
    Column("strlist_type", datatype.strlist, default=StrList(["x", "y", "z"])),
    Column("intlist_type", datatype.intlist, default=IntList([8, 9, 10])),
    Column("pickletype_type", datatype.pickletype, default={1:"a", 2:"b", 3:"c"}),
    )

metadata.create_all(engine)

def metadata_test():
    """create a new metadata, try to reflect from a engine.
    you can do:
        metadata1 = MetaData(engine)
    or
        metadata1 = MetaData()
        metadata1.reflect(engine)
    """
    metadata1 = MetaData(engine)
    print(metadata1)
    
metadata_test()
