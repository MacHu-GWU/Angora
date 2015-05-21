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
    def test_create_all(self):
        self.assertEqual(len(metadata.tables), 2)
        
    def test_get_table(self):
        self.assertIsInstance(metadata.get_table("table1"), Table)
        
    def test_reflect(self):
        metadata1 = MetaData()
        metadata1.reflect(engine)
        self.assertEqual(len(metadata1.tables), 2)
        
unittest.main()
