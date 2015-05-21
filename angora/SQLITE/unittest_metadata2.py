##encoding=utf-8

from core import *
import unittest

metadata = MetaData()
datatype = DataType()
engine = Sqlite3Engine(":memory:")
table1 = Table("table1", metadata, Column("column1", datatype.text, primary_key=True))
table2 = Table("table2", metadata, Column("column1", datatype.integer, nullable=False))
metadata.create_all(engine)

class MetaDataUnittest(unittest.TestCase):
    def test_drop_all(self):
        table_name_list = list(engine.execute("""SELECT name FROM sqlite_master 
            WHERE type='table';"""))
        self.assertEqual(len(table_name_list), 2) # before, have 2 tables

        metadata.drop_all(engine)
        table_name_list = list(engine.execute("""SELECT name FROM sqlite_master 
            WHERE type='table';"""))
        self.assertEqual(len(table_name_list), 0) # after, no table
        
unittest.main()