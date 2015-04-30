##encoding=utf-8

from csvfile import CSVFile
from sheet import Sheet
from angora.GADGET.pytimer import Timer
from angora.SQLITE import *
import pandas as pd
import sqlite3

timer = Timer()

def hard_way():
    """从csv文件中将数据读入数据库
    """
    connect = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES)
    cursor = connect.cursor()
    cursor.execute("""CREATE TABLE employee (employee_id TEXT, age INTEGER, height REAL,
                    enroll_date DATE, create_datetime TIMESTAMP)""")
    
    
    df = pd.read_csv(r"testdata\bigcsvfile.txt",
                     sep=",",
                     header=0,
                     parse_dates=[3,4],
                     )
 
    for _, series in df.iterrows():
        series["CREATE_DATE"] = series["CREATE_DATE"].to_datetime().date()
        series["CREATE_DATETIME"] = series["CREATE_DATETIME"].to_datetime()
        try:
            cursor.execute("INSERT INTO employee VALUES (?,?,?,?,?)", tuple(series))
        except:
            pass

    for record in cursor.execute("SELECT * FROM employee"):
        print(record)

hard_way()

def easy_way():
    metadata = MetaData()
    engine = Sqlite3Engine(":memory:")
    datatype = DataType()
    employee = Table("employee", metadata,
                Column("employee_id", datatype.text, primary_key=True),
                Column("age", datatype.integer),
                Column("height", datatype.real),
                Column("enroll_date", datatype.date),
                Column("create_datetime", datatype.datetime),
                )
    metadata.create_all(engine)
    ins = employee.insert()
    
    timer.start()
    # === real work ===
    sheet = Sheet({"employee_id": "TEXT", "age": "INTEGER", "height": "REAL",
                   "enroll_date": "DATE", "create_datetime": "DATETIME"})
    
    csvfile = CSVFile(r"testdata\bigcsvfile.txt",
                      sep=",",
                      header=True,
                      dtype={"CREATE_DATE": "DATE", "CREATE_DATETIME": "DATETIME"})
    sheet.match(csvfile.sample)
    
    for row in csvfile.generate_rows():
        try:
            engine.insert_row(ins, Row.from_dict(sheet.convert(row)))
        except:
            pass
    timer.timeup()
    engine.prt_all(employee)

easy_way()