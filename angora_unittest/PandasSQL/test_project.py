##encoding=utf8

from __future__ import print_function
from angora.PandasSQL.sqlite3blackhole import Sqlite3BlackHole, CSVFile

def main():
    bh = Sqlite3BlackHole(":memory:") # define database
    # define first file
    advertisement = CSVFile(r"test_data/advertisement.txt", # path
                            table_name="advertisement", # table_name
                            sep=",", # seperator
                            header=True,
                            dtype={"id": "TEXT", "hour": "DATETIME"}, # datatype
                            usecols=[0,1,2,3,4,5,6,7,8,9], # use cols
                            primary_key_columns=["id"]) # primary_key_columns
    # define second file
    employee1 = CSVFile(r"test_data/employee1.txt",
                       table_name="employee",
                       sep=",",
                       header=True,
                       dtype={"employee_id": "TEXT", "start_date": "DATE"},
                       primary_key_columns=["employee_id"])
    
    # define third file
    employee2 = CSVFile(r"test_data/employee2.txt",
                       table_name="employee",
                       sep=",",
                       header=True,
                       dtype={"employee_id": "TEXT", "start_date": "DATE"},
                       primary_key_columns=["employee_id"])
    bh.add(advertisement)
    bh.add(employee1)
    bh.add(employee2)
    bh.devour()
    
    bh.engine.prt_howmany(advertisement.table)
    bh.engine.prt_howmany(employee1.table)
    
    print(repr(advertisement.table))
    print(repr(employee1.table))
    
# main()

def non_primary_key():
    """没有primary_key的情况
    """
    bh = Sqlite3BlackHole(":memory:")
    employee1 = CSVFile(r"test_data/employee1.txt",
                       table_name="employee",
                       sep=",",
                       header=True,
                       dtype={"employee_id": "TEXT", "start_date": "DATE"},
                       )
    employee2 = CSVFile(r"test_data/employee2.txt",
                       table_name="employee",
                       sep=",",
                       header=True,
                       dtype={"employee_id": "TEXT", "start_date": "DATE"},
                       )
    bh.add(employee1)
    bh.add(employee2)
    bh.devour()
#     bh.update()
    bh.engine.prt_all(employee1.table)
    print(repr(employee1.table))
    
non_primary_key()

def non_header():
    """没有header的情况
    """
    bh = Sqlite3BlackHole(":memory:")
    employee1 = CSVFile(r"test_data/nonheader_employee1.txt",
                       table_name="employee",
                       sep=",",
                       dtype={0: "TEXT", 2: "DATE"},
                       primary_key_columns=[0],
                       )
    
    employee2 = CSVFile(r"test_data/nonheader_employee2.txt",
                       table_name="employee",
                       sep=",",
                       dtype={0: "TEXT", 2: "DATE"},
                       primary_key_columns=[0],
                       )
    bh.add(employee1)
    bh.add(employee2)
    bh.devour()
#     bh.update()
    bh.engine.prt_all(employee1.table)
    
    print(repr(employee1.table))
    
# non_header()
