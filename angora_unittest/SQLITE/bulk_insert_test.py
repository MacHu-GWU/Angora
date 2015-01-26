##encoding=utf8

from __future__ import print_function

def iterC(cursor, arraysize = 10):
    "An iterator that uses fetchmany to keep memory usage lower"
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result
            
def prt_all(cursor):
    """equivalent to:
    for row in c.fetchall():
        print(row)
    """
    counter = 0
    for row in iterC(cursor):
        print(row)
        counter += 1
    print("Found %s records" % counter)

def stable_insertmany(connect, cursor, sqlcmd, records):
    """
    example valid sql command:
        INSERT INTO tablename VALUES (?, ?, ...)
    """
    try:
        cursor.executemany(sqlcmd, records)
    except: # failed to bulk insert, try normal iteratively insert
        for record in records:
            try:
                cursor.execute(sqlcmd, record)
            except:
                pass
    connect.commit()

if __name__ == "__main__":
    from angora.GADGET.pytimer import Timer
    from angora.DATA.iterable import grouper_list
    from math import sqrt
    import sqlite3
    import random
    import os
        
    def bulk_insert_test():
        """
        当待插入的数据中只有少量数据会损害数据完整性时, 如果我们一条条insert and commit会太慢, 而我们又
        无法一次性insertmany and commit。此时我们如果能将所有数据拆分成大小为 平方根(数据总条数) 的小包,
        其中就会有许多小包都能insertmany成功, 而只有部分小包必须一条条的insert and commit。这样总体速度
        就会得到极大的提高。
        
        为了测试该算法, 我们设定primary key为1 - 10,000。我们预先向数据库内随机插入100条数据。
        然后我们尝试将10000条primary key分别为1 - 10,000的数据插入数据库:
        
        注: sqlite引擎不需要在出错后进行rollback, 所以没有必要在每一次try语句失败后进行rollback, 所以
        该算法的优势体现不出来。为了达到展示算法的目的, 我们强行对每一次insert/insertmany进行commit。
        """
        
        timer = Timer()
        try:
            os.remove("test.db")
        except:
            pass
        
        connect = sqlite3.connect("test.db")
        cursor = connect.cursor()
        cursor.execute("CREATE TABLE test (uuid INTEGER, name TEXT, PRIMARY KEY (uuid) );")
        
        ### 向数据库中预先填充部分数据
        complexity = 100
        records = [(random.randint(1, complexity**2), "abcdefghijklmnopqrstuvwxyz" ) for i in range(complexity)]
        for record in records:
            try:
                cursor.execute("INSERT INTO test VALUES (?,?);", record )
            except:
                pass
        connect.commit()
    
        records = [(i, "abcdefghijklmnopqrstuvwxyz") for i in range(1, complexity**2 + 1)]
        
        def insert1(): # 90.7 seconds/0.9448 seconds, 100/10 complexity
            """尝试插入一条commit一条
            """
            for record in records:
                try:
                    cursor.execute("INSERT INTO test VALUES (?,?);", record )
                    connect.commit()
                except:
                    pass
        
#         timer.test(insert1, 1)
        
        def insert2(): # 31.4067297608 seconds/0.38 seconds, 100/10 complexity
            """尝试把大数据包分拆成 size = sqft(len(data)) 大小的小包, 再尝试将小包整包insert, 如果
            不成功, 则逐条插入
            """
            try:
                cursor.executemany("INSERT INTO test VALUES (?,?);", records)
                connect.commit()
            except:
                for chunk in grouper_list(records, int(sqrt(len(records)))):
                    try:
                        cursor.executemany("INSERT INTO test VALUES (?,?);", chunk)
                        connect.commit()
                    except:
                        for record in chunk:
                            try:
                                cursor.execute("INSERT INTO test VALUES (?,?);", record )
                                connect.commit()
                            except:
                                pass
        
#         timer.test(insert2, 1)
        
        def insert3(): # 8.67 seconds
            """连续两次分拆数据包, 尝试进一步优化速度
            """
            try:
                cursor.executemany("INSERT INTO test VALUES (?,?);", records)
                connect.commit()
            except:
                for chunk in grouper_list(records, int(sqrt(len(records)))):
                    try:
                        cursor.executemany("INSERT INTO test VALUES (?,?);", chunk)
                        connect.commit()
                    except:
                        for smaller_chunk in grouper_list(chunk, int(sqrt(len(chunk)))):
                            try:
                                cursor.executemany("INSERT INTO test VALUES (?,?);", smaller_chunk)
                                connect.commit()
                            except:
                                for record in smaller_chunk:
                                    try:
                                        cursor.execute("INSERT INTO test VALUES (?,?);", record )
                                        connect.commit()
                                    except:
                                        pass
        timer.test(insert3, 1)
        
    bulk_insert_test()