##encoding=utf8

from angora.DBA import *

class Excel2db_unittest():
    @staticmethod
    def excel2sqlite():
        excel2sqlite(r"testfile\wc3_demo_db.xlsx")
        
if __name__ == "__main__":
    Excel2db_unittest.excel2sqlite()