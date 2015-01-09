##encoding=utf8

"""
Import
    from angora.DBA.POSTGRES import postgreswrapper as psql
"""

from __future__ import print_function
from angora.DATA import grouper_list
from math import sqrt
import psycopg2

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
                    
def lazy_insertmany(connect, cursor, sqlcmd, records):
    """通常数据中有大量的有可能违背数据完整性时使用。
    example valid sql command:
        INSERT INTO #tablename VALUES (%s, %s, ...)
    """
    for record in records:
        try:
            cursor.execute(sqlcmd, record)
            connect.commit()
        except psycopg2.IntegrityError:
            connect.rollback()
            
def smart_insertmany(connect, cursor, sqlcmd, records):
    """通常数据中只有非常少量的有可能违背数据完整性时使用。
    example valid sql command:
        INSERT INTO #tablename VALUES (%s, %s, ...)
    """
    for chunk in grouper_list(records, int(sqrt(len(records))) ): 
        try:
            cursor.executemany(sqlcmd, chunk)
            connect.commit()
        except:
            connect.rollback()
            for record in chunk:
                try:
                    cursor.execute(sqlcmd, record)
                    connect.commit()
                except psycopg2.IntegrityError:
                    connect.rollback()
                        
def lucky_insertmany(connect, cursor, sqlcmd, records):
    """有很大可能性能一次全部insert成功时使用。
    example valid sql command:
        INSERT INTO #tablename VALUES (%s, %s, ...)
    """
    try:
        cursor.executemany(sqlcmd, records)
        connect.commit()
    except psycopg2.IntegrityError:
        connect.rollback()
        for record in records:
            try:
                cursor.execute(sqlcmd, record)
                connect.commit()
            except psycopg2.IntegrityError:
                connect.rollback()