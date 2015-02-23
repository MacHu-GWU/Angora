import sqlite3
import datetime

conn = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES)
c = conn.cursor()
c.execute("CREATE TABLE test (value1 timestamp, value2 DATE)")
c.execute("INSERT INTO test (value1, value2) VALUES (?, ?)", (datetime.datetime.now(), datetime.date(2010,1,1)))
conn.commit()
print(c.execute("SELECT * FROM test").fetchall())

# import sqlite3
# import datetime
# 
# con = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
# cur = con.cursor()
# cur.execute("create table test(d date, ts timestamp)")
# 
# today = datetime.date.today()
# now = datetime.datetime.now()
# 
# cur.execute("insert into test(d, ts) values (?, ?)", (today, now))
# cur.execute("select d, ts from test")
# row = cur.fetchone()
# print(today, "=>", row[0], type(row[0]))
# print(now, "=>", row[1], type(row[1]))
# 
# cur.execute('select current_date as "d [date]", current_timestamp as "ts [timestamp]"')
# row = cur.fetchone()
# print("current_date", row[0], type(row[0]))
# print("current_timestamp", row[1], type(row[1]))