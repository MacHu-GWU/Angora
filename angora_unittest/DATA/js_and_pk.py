##encoding=utf8

from __future__ import print_function
from angora.DATA import *
import os

class Js_unittest():
    @staticmethod
    def everything():
        """test three main function in js:
        load_js, dump_js, prt_js
        """
        print("{:=^100}".format("json_everything"))
        data = {1: ["a1", "a2"], 2: ["b1", "b2"]}   # 初始化数据
        dump_js(data, "data.json", replace = True)  # 测试 dump_js
        print ( load_js("data.json") )              # 测试 load_js
        prt_js(data)                                # 测试 prt_js
        
        try:
            dump_js(data, "data.json")
        except Exception as e:
            print(e)
                
        os.remove("data.json")                      # 清除掉残留文件
      
    @staticmethod
    def safe_dump_js():
        data = {i:"a" for i in range(1000000)}
        fname = "data.json"
        dump_js(data, fname, replace = True)        # 首先尝试dump
        safe_dump_js(data, fname)                   # 然后以同样的文件名dump，看看会不会出现临时文件
    
class Pk_unittest():
    @staticmethod
    def everything():
        """test two main function in js:
        load_pk, dump_pk
        """
        print("{:=^100}".format("pickle_everything"))
        obj = {1: ["a1", "a2"], 2: ["b1", "b2"]}    # 初始化数据
        dump_pk(obj, "obj.p", 2, replace = True)    # 测试 dump_pk
        print(load_pk("obj.p") )                    # 测试 load_pk
        
        try:
            dump_pk(obj, "obj.p", 2) 
        except Exception as e:
            print(e)
            
        os.remove("obj.p")                          # 清除掉残留文件

    @staticmethod
    def safe_dump_pk():
        data = {i:"a" for i in range(1000000)}
        fname = "data.p"
        dump_pk(data, fname, replace = True)        # 首先尝试dump
        safe_dump_pk(data, fname)                   # 然后以同样的文件名dump，看看会不会出现临时文件

    @staticmethod
    def pk_vs_database1():
        """pickle.dumps的结果是bytes, 而在python2中的sqlite不支持bytes直接插入数据库,
        必须使用base64.encode将bytes编码成字符串之后才能存入数据库。
        而在python3中, 可以直接将pickle.dumps的bytestr存入数据库, 这样就省去了base64编码的开销
        """
        import sqlite3
        
        conn = sqlite3.connect(":memory:")
        c = conn.cursor()
        c.execute("CREATE TABLE test (dictionary BLOB) ") # BLOB is byte
        c.execute("INSERT INTO test VALUES (?)", 
                  (obj2bytestr({1:"a", 2:"你好"}),))
        
        print(c.execute("select * from test").fetchone()) # see what stored in database
        print(bytestr2obj(c.execute("select * from test").fetchone()[0])) # recovery object from byte str

    @staticmethod
    def pk_vs_database2():
        import sqlite3
        
        conn = sqlite3.connect(":memory:")
        c = conn.cursor()
        c.execute("CREATE TABLE test (name TEXT) ")
        c.execute("INSERT INTO test VALUES (?)", 
                  (obj2str({1:"a", 2:"你好"}),))
        
        print(c.execute("select * from test").fetchone()) # see what stored in database
        print(str2obj(c.execute("select * from test").fetchone()[0])) # recovery object from text str

def clear_residue():
    os.remove("data.json")
    os.remove("data.p")
    
if __name__ == "__main__":
    Js_unittest.everything()
    Js_unittest.safe_dump_js()
    Pk_unittest.everything()
    Pk_unittest.safe_dump_pk()
    Pk_unittest.pk_vs_database1()
    Pk_unittest.pk_vs_database2()
    
    try:
        clear_residue()
    except:
        pass