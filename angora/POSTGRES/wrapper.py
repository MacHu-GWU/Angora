##encoding=utf8

"""
import:
    from angora.POSTGRES.wrapper import iterC, prt_all, lazy_insertmany, smart_insertmany, genius_insertmany, lucky_insertmany
"""

from __future__ import print_function
from angora.DATA.iterable import grouper_list
from angora.GADGET.logger import Messenger
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
                    
def genius_insertmany(connect, cursor, sqlcmd, records):
    """采用二次分包优化
    """
    messenger = Messenger()
    todo_counter, success_counter, failed_counter = len(records), 0, 0
    for chunk in grouper_list(records, int(sqrt(len(records))) ): 
        messenger.show("%s todo, %s success, %s failed" % (todo_counter, success_counter, failed_counter))
        try:
            cursor.executemany(sqlcmd, chunk)
            connect.commit()
            todo_counter -= len(chunk)
            success_counter += len(chunk)
            
        except psycopg2.IntegrityError:
            connect.rollback()
            
            for smaller_chunk in grouper_list(chunk, int(sqrt(len(chunk))) ):
                try:
                    cursor.executemany(sqlcmd, chunk)
                    connect.commit()
                    todo_counter -= len(chunk)
                    success_counter += len(chunk)
                    
                except psycopg2.IntegrityError:
                    connect.rollback()
                    
                    for record in smaller_chunk:
                        try:
                            cursor.execute(sqlcmd, record)
                            connect.commit()
                            todo_counter -= 1
                            success_counter += 1
                            
                        except psycopg2.IntegrityError:
                            connect.rollback()
                            failed_counter += 1
        
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