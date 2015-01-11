##encoding=utf8

from __future__ import print_function
import sqlite3

class TEXT():
    name = "TEXT"

class INTEGER():
    name = "INTEGER"

class REAL():
    name = "REAL"

class DATETIME():
    name = "DATETIME"

class DATE():
    name = "DATE"

class NULL():
    name = "NULL"

class Constraint():
    def __str__(self):
        return self.name

class PrimaryKey(Constraint):
    name = "PRIMARY KEY"
    
class NotNull(Constraint):
    name = "NOT NULL"

class Column():
    def __init__(self, name, dtype, primary_key=False, not_null=False):
        self.name = name
        self.dtype = dtype
        self.primary_key = primary_key
        
        
        self.constraint = list()
        if not_null:
            self.constraint.append(NotNull())
                
    def sqlcmd(self):
        """返回Column在CREATE TABLE中的定义的那部分字符串
        例如:
        Column("movieID", TEXT, primary_key=True)
        
        """
        return "\t%s" % " ".join([self.name, 
                         self.dtype.name, 
                         " ".join([str(constraint) for constraint in self.constraint]) ]).strip()
    
class Table():
    def __init__(self, name, engine, *args):
        self.name = name
        self.columns = args
        self.columnnames = [column.name for column in args]
        engine.tables.append(self)
        
class Engine():
    def __init__(self):
        self.connect = None
        self.cursor = None
        self.tables = list()

    """下面是由SqlAlchemy所生成的Sql语句, 请参考
    CREATE TABLE movie (
        movie_id INTEGER NOT NULL, 
        title TEXT NOT NULL, 
        release_date DATE, 
        length INTEGER, 
        PRIMARY KEY (movie_id)
    )
    """

    def create_all(self):
        """
        """
        for table in self.tables:
            
            column_sqlcmd_list = list()
            primary_key_list = list()
            for column in table.columns:
                column_sqlcmd_list.append(column.sqlcmd())
                if column.primary_key:
                    primary_key_list.append(column.name)
            
            
            
            print(  ",\n".join(column_sqlcmd_list)  )
            print( ",\n\tPRIMARY KEY (%s)" % ", ".join(primary_key_list) )
#             cmd_primary_key = 
            cmd = "CREATE TABLE {0} (\n{1}{2}\n);".format(table.name,
                       ",\n".join(column_sqlcmd_list),
                       ",\n\tPRIMARY KEY (%s)" % ", ".join(primary_key_list)
                       )
            print(cmd)
            
#             cmd_create_table = "CREATE TABLE %s " % table.name
#             cmd_define_columns = "(\n%s\n);" % ",\n".join( [column.sqlcmd() for column in table.columns] )
#             print(cmd_create_table + cmd_define_columns)
            
def create_engine(dbname):
    engine = Engine()
    engine.connect = sqlite3.connect(dbname)
    engine.cursor = engine.connect.cursor()
    return engine

if __name__ == "__main__":
    
#     column = Column("movieID", TEXT, primary_key=True)
#     column = Column("title", TEXT)
#     column = Column("release_date", DATE)
#     column = Column("length", INTEGER, not_null=True)
#     print([ column.sqlcmd() ])
    
    engine = create_engine(":memory:")
    documents = Table("documents", engine,
                      Column("movieID", TEXT, primary_key=True),
                      Column("title", TEXT),
                      Column("release_date", DATE),
                      Column("length", INTEGER, not_null=True),
                      )
    
#     print(engine.tables)
    engine.create_all()