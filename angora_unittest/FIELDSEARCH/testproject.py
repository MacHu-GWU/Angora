##encoding=utf8

"""
本项目是在 FieldSearchEngine 的框架上进行的项目。采用了该框架后, 创建一个使用单元搜索引擎搜索文档的应用
只需要如下几步:

    1. 定义schema, create_schema()函数既是, 具体详细的定义请参考 field.py 和 基于sqlite3的单元搜索引擎.md
    2. 从你的原始数据中读取文档, 按照你定义的schema, 将文档封装成字典, 再装入数据库。load_document()既是
    3. 用引擎创建一个查询, 添加各种过滤条件, 然后执行查询, 返回用字典封装的document
    
    基本上可以控制在 30 - 40 行代码以内即可建立一个搜索应用

prerequisites: sqlalchemy 目前最新版 0.9.0

compatibility: only compatible to python3

usage:
    拉倒脚本最下面, 注释掉2, 3, 然后一条条的逐次执行
    
    step1_initialize_project() # 准备数据库
    step2_display_help_information() # 填充信息
    step3_test_query() # 尝试查询

"""

from angora.FIELDSEARCH.fields import *
from angora.FIELDSEARCH.engine import *
from angora.DATA.js import load_js
from angora.DATA.timewrapper import TimeWrapper
import os
import time

def reset():
    try:
        os.remove("test.db")
    except:
        pass
    
def load_documents():
    """从 raw data 中读取原始数据, 并封装成一条条的文档
    """
    tw = TimeWrapper()
    documents = list()
    fname = r"avdb_cn.json"
    avdb = load_js(fname)
    for ID, av in avdb.items():
        document = dict()
        document["avid"] = ID
        document["title"] = av["TITLE"]
        document["artists"] = "&".join(av["ARTIST"].values() )
        document["tags"] = "&".join(av["TAG"].values() )
        document["release_date"] = tw.str2date(av["DATE"])
        document["length"] = int(av["LENGTH"])
        document["maker"] = av["MAKER"]
        document["distributor"] = av["DISTRIBUTOR"]
        
        documents.append(document)
    return documents

def create_schema():
    av_schema = Schema("av",
        Field("avid", Searchable_UUID, Searchable_TEXT, primary_key=True),
        Field("title", Searchable_TEXT),
        Field("artists", Searchable_KEYWORD),
        Field("tags", Searchable_KEYWORD),
        Field("release_date", Searchable_DATE),
        Field("length", Searchable_INTEGER),
        Field("maker", Searchable_TEXT),
        Field("distributor", Searchable_TEXT),
    )
    return av_schema

def step1_initialize_project():
    reset()
    documents = load_documents()
    engine.add_all(documents)
    
def step2_display_help_information():
    """由于有可能被所有的演员名字刷屏, 建议一条一条的试验
    """
    engine.help()

#     engine.display_searchable_fields()
#     engine.display_criterion()
#     engine.display_keyword_fields()

#     engine.display_valid_keyword("artists")
#     engine.display_valid_keyword("tags")
    
#     engine.search_valid_keyword("artists", "吉沢")
#     engine.search_valid_keyword("tags", "女")

def step3_test_query():
    """query的criterion默认是以逻辑AND连接
    建议尝试各种criterion的排列组合, 看看结果是否正确
    """
    query = engine.create_query()
    query.add(QueryEqual("avid", "SAMA-730"))
#     query.add(QueryStartwith("avid", "SOE") ) # 番号以SOE公司开头
#     query.add(QueryBetween("release_date", "2013-01-01", "2013-12-31")) # 2013年内
#     query.add(QueryContains("tags", "单体作品", "女教师")) # 标签包含有 单体作品 和 女教师 两个
#     query.add(QueryContains("artists", "吉沢明歩")) # "吉沢明歩" 参演
#     query.add(QueryEqual("artists", "吉沢明歩")) # 演员有且只有 "吉沢明歩"
    
    st = time.clock()
    counter = 0
    for document in engine.search_document(query):
        counter += 1
        print(document)
    print("搜索到 %s 条结果" % counter) # 246973
    print("耗时 %s 秒" % (time.clock() - st) )

av_schema = create_schema() # 这两条不要动
engine = create_search_engine("test.db", av_schema)

step1_initialize_project()
# step2_display_help_information()
step3_test_query()