##encoding=utf-8

"""
Copyright (c) 2015 by Sanhe Hu
------------------------------
    Author: Sanhe Hu
    Email: husanhe@gmail.com
    Lisence: LGPL


Module description
------------------
    tala is a fast, small and portable document search engine.
    set up the database is super easy:
        1. define the Schema, Fields
        2. define the data transfer generator function
        3. tala! your app is ready to use
        
    tala provide a human readable, friendly query gives user flexibility and power to search
    documents in a easy way. no SQL language is needed.
        1. create a query.
        2. add some criterion
        3. tala!
    
    
Keyword
-------
    search engine, sqlite3, database


Compatibility
-------------
    Python2: No
    Python3: Yes
    
    
Prerequisites
-------------
    angora.SQLITE


Import Command
--------------
    from angora.TALA.tala import FieldType, Field, Schema, SearchEngine
"""

from __future__ import print_function
from angora.SQLITE.core import Row, Select, DataType, MetaData, Column, Table, Sqlite3Engine
from angora.DATA.dtype import OrderedSet
from angora.DATA.iterable import grouper
from collections import OrderedDict

##################################################
#                                                #
#               Field type class                 #
#                                                #
##################################################

datatype = DataType()

class SEARCHABLE_TYPE():
    """
    [EN]base class for all field type class
    [CN]所有搜索单元类型的父类
    """
    def __str__(self):
        return self.name
    
    def __repr__(self):
        return "%s()" % self.name

class _Searchable_UUID(SEARCHABLE_TYPE):
    """整个Schema中有且只能有一个Field属于此类; 字符串精确匹配"""
    name = "_Searchable_UUID"
    sqlite_dtype = datatype.text
    sqlite_dtype_name = "TEXT"
    is_pickletype = False
    
class _Searchable_ID(SEARCHABLE_TYPE):
    """字符串精确匹配"""
    name = "_Searchable_ID"
    sqlite_dtype = datatype.text
    sqlite_dtype_name = "TEXT"
    is_pickletype = False
    
class _Searchable_TEXT(SEARCHABLE_TYPE):
    """模糊字符串匹配"""
    name = "_Searchable_TEXT"
    sqlite_dtype = datatype.text
    sqlite_dtype_name = "TEXT"
    is_pickletype = False
    
class _Searchable_KEYWORD(SEARCHABLE_TYPE):
    """字符串集合匹配"""
    name = "_Searchable_KEYWORD"
    sqlite_dtype = datatype.pythonset
    sqlite_dtype_name = "PYTHONSET"
    is_pickletype = True
    
class _Searchable_DATE(SEARCHABLE_TYPE):
    """日期逻辑匹配"""
    name = "_Searchable_DATE"
    sqlite_dtype = datatype.date
    sqlite_dtype_name = "DATE"
    is_pickletype = False
    
class _Searchable_DATETIME(SEARCHABLE_TYPE):
    """日期时间逻辑匹配"""
    name = "_Searchable_DATETIME"
    sqlite_dtype = datatype.datetime
    sqlite_dtype_name = "TIMESTAMP"
    is_pickletype = False
    
class _Searchable_INTEGER(SEARCHABLE_TYPE):
    """整数逻辑匹配"""
    name = "_Searchable_INTEGER"
    sqlite_dtype = datatype.integer
    sqlite_dtype_name = "INTEGER"
    is_pickletype = False
    
class _Searchable_REAL(SEARCHABLE_TYPE):
    """实数逻辑匹配"""
    name = "_Searchable_REAL"
    sqlite_dtype = datatype.real
    sqlite_dtype_name = "REAL"
    is_pickletype = False
    
class _Unsearchable_OBJECT(SEARCHABLE_TYPE):
    """无法被搜索到, 可以储存任意python对象"""
    name = "_Unsearchable_OBJECT"
    sqlite_dtype = datatype.pickletype
    sqlite_dtype_name = "PICKLETYPE"
    is_pickletype = True
    
class FieldType():
    Searchable_UUID = _Searchable_UUID()
    Searchable_ID = _Searchable_ID()
    Searchable_TEXT = _Searchable_TEXT()
    Searchable_KEYWORD = _Searchable_KEYWORD()
    Searchable_DATE = _Searchable_DATE()
    Searchable_DATETIME = _Searchable_DATETIME()
    Searchable_INTEGER = _Searchable_INTEGER()
    Searchable_REAL = _Searchable_REAL()
    Unsearchable_OBJECT = _Unsearchable_OBJECT()


##################################################
#                                                #
#                 Field class                    #
#                                                #
##################################################

class Field():
    """Field可能有多种SEARCHABLE_TYPE, 但是只能有一种sqlite_dtype
    所以必须保证Field.search_types的sqlite_dtype都相同。
    """
    def __init__(self, field_name, *list_of_searchable_type, primary_key=False, nullable=True, default=None):
        """
        Field.field_name
            string type, 单元名称, 对应数据表中的column_name 
        
        Field.sqlite_dtype
            the sqlite3 data type in python sqlite3
            
        Field.is_pickletype
            whether it is stored in pickle byte string
        
        Field.primary_key
            boolean type, 该列在数据库知否是PRIMARY KEY
        
        Field.nullable
            boolean type, 该列是否允许为NULL
            
        Field.default
            any support type defined in search_types, 该列的默认值

        Field.search_types
            dict type, {SEARCHABLE_TYPE.name: SEARCHABLE_TYPE} 同一列可以以多种方式被搜索到
        """
        self.field_name = field_name
        self.sqlite_dtype = list_of_searchable_type[0].sqlite_dtype
        self.is_pickletype = list_of_searchable_type[0].is_pickletype
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default

        self.search_types = {searchable_type.name: searchable_type \
                             for searchable_type in list_of_searchable_type}
        
        self.self_validate()
    
    def self_validate(self):
        sqlite_dtype_set = {searchable_type.sqlite_dtype_name \
                            for searchable_type in self.search_types.values()}
        if len(sqlite_dtype_set) != 1:
            raise Exception("%s have different sqlite dtype!" % list(self.search_types.keys()))
        
    def __str__(self):
        return "Field(field_name=%s, search_types=%s, primary_key=%s, nullable=%s, default=%s)" % (
            repr(self.field_name),
            self.search_types,
            self.primary_key,
            self.nullable,
            repr(self.default),
            )
    
    def __repr__(self):
        return "Field(%s, %s, primary_key=%s, nullable=%s, default=%s)" % (
            repr(self.field_name),
            ", ".join([repr(fieldtype) for fieldtype in self.search_types.values() ]),
            self.primary_key,
            self.nullable,
            repr(self.default),
            )


##################################################
#                                                #
#                 Schema class                   #
#                                                #
##################################################

class Schema():
    """
    有且只有一个Field的search_types中包含Searchable_ID, 而且该Field必须是PRIMARY KEY
    """
    def __init__(self, schema_name, *Fields):
        """
        Schema.schema_name 
            string type, 储存主表的名称
            
        Schema.uuid
            string type, 储存全局主键的 单元/列 的名称
            
        Schema.keyword_fields
            list type, 储存关键字类的 单元/列 的名称

        Schema.fields
            OrderedDict type, 储存 {Field名称: Field对象} 的字典
        """
        # get self.schema_name
        self.schema_name = schema_name
        
        # get self.uuid, self.keyword_fields, fields
        self.uuid = None
        self.keyword_fields = list()
        self.fields = OrderedDict()
        for field in Fields: # OrderedDict({field.name: field object})
            self.fields[field.field_name] = field
            if "_Searchable_UUID" in field.search_types:
                self.uuid = field.field_name
            if "_Searchable_KEYWORD" in field.search_types:
                self.keyword_fields.append(field.field_name)
                
        self.self_validate()

    def self_validate(self):
        """检查这个Schema的设置是否合理。即满足:
        1. 有且只有一个uuid的单元
        """
        uuid_fields_count = 0
        for field in self.fields.values():
            if "_Searchable_UUID" in field.search_types:
                uuid_fields_count += 1
                
        if uuid_fields_count == 0:
            raise Exception("You don't have any uuid field for your document!")
        elif uuid_fields_count >= 2:
            raise Exception("One and only one Fields can have only one Searchable_UUID!")
        
    def __str__(self):
        return self.schema_name
    
    def __repr__(self):
        return "Schema(%s, %s)" % (
            repr(self.schema_name), 
            ", ".join([repr(field) for field in self.fields.values() ]),
            )
        
    def prettyprint(self):
        """pretty print schema detail
        """
        print(
              "Schema(%s,\n\t%s)" % (
                repr(self.schema_name), 
                ",\n\t".join([repr(field) for field in self.fields.values() ]),
                )
              )


##################################################
#                                                #
#              SearchEngine class                #
#                                                #
##################################################

class SearchEngine():
    def __init__(self, schema):
        self.schema = schema
        self.database = schema.schema_name + ".db"
        self.engine = Sqlite3Engine(self.database)
        self.engine.autocommit(False)
        self.metadata = MetaData()
        self.create_all_tables()
    
    def create_all_tables(self):
        ## create the main table
        self.metadata = MetaData()
        main_table_columns = [Column(field.field_name, field.sqlite_dtype,
                                 primary_key = field.primary_key,
                                 nullable = field.nullable,
                                 default = field.default,) \
                              for field in self.schema.fields.values()]
        
        self.main_table = Table(self.schema.schema_name, self.metadata, *main_table_columns)
        
        ## create keyword table
        for keyword_field in self.schema.keyword_fields:
            Table(keyword_field, self.metadata,
                  Column("keyword", datatype.text, primary_key = True),
                  Column("uuid_set", datatype.pythonset),
                  )
        
        self.metadata.create_all(self.engine)
        
    def get_table(self, table_name):
        """根据table_name得到一个表对象
        """
        return self.metadata.tables[table_name]
    
    def add_one(self, document):
        """用于往数据库中添加数据, 以增量更新的模式更新索引, 性能较低
        1. 往主表格中填充一条文档
        2. 更新倒排索引里的表中的索引数据
        """
        # 将字典document转化为row
        columns, values = list(), list()
        for k, v in document.items():
            columns.append(k)
            values.append(v)
        row = Row(columns, values)
        
        # 更新主表的数据
        ins = self.main_table.insert()
        self.engine.insert_row(ins, row)
        
        # 对每一个field所对应的表进行更新
        for keyword_field in self.schema.keyword_fields:
            table = self.get_table(keyword_field)
            ins = table.insert()
            upd = table.update()
              
            # 对表中所涉及的keyword的行进行更新
            for keyword in document[keyword_field]:
                try: # 尝试插入新的, 若已经存在, 则进入更新
                    self.engine.insert_row(ins, Row(
                                                    ["keyword", "uuid_set"], 
                                                    [keyword, set([document[self.schema.uuid]])]
                                                     ) )
                except: # 对keyword的行进行更新
                    a = self.engine.select(Select([table.uuid_set]).where(table.keyword == keyword))
                    
                    new_uuid_set = list(a)[0][0]
                    
                    print(new_uuid_set)
                    new_uuid_set.add(document[self.schema.uuid])
                      
                    self.engine.update(upd.values(uuid_set = new_uuid_set).where(table.keyword == keyword) )
                    
    def clone_from_data_stream(self, documents):
        """用于从0开始, 从批量文档中生成数据库, 性能较高
        1. 往主表格中填充一条文档
        2. 从文档中生成倒排索引的索引字典
        3. 往所有的索引表中填充索引
                
        invert_index = {keyword: set_of_uuid}
        """
        import time
        st = time.clock()
        print("正在往数据库 %s 中填充数据..." % self.schema.schema_name)
        
        # 初始化all_inv_dict
        all_inv_dict = dict()
        for keyword_field in self.schema.keyword_fields: # initialize a empty dict for invert index
            all_inv_dict[keyword_field] = dict()
            
        ins = self.main_table.insert()
        counter = 0
        for document in documents:
            counter += 1
            # 将字典document转化为row
            columns, values = list(), list()
            for k, v in document.items():
                columns.append(k)
                values.append(v)
            row = Row(columns, values)
        
            # 更新主表的数据
            try:
                self.engine.insert_row(ins, row)
            except:
                pass
            
            # 计算倒排索引
            for keyword_field in self.schema.keyword_fields:
                uuid = document[self.schema.uuid]
                for keyword in document[keyword_field]:
                    if keyword in all_inv_dict[keyword_field]:
                        all_inv_dict[keyword_field][keyword].add(uuid)
                    else:
                        all_inv_dict[keyword_field][keyword] = set([uuid,])
        
        # 将all_inv_dict中的数据存入索引表中
        for keyword_field in all_inv_dict:
            table = self.get_table(keyword_field)
            ins = table.insert()
            for keyword, uuid_set in all_inv_dict[keyword_field].items():
                self.engine.insert_record(ins, (keyword, uuid_set))
        
        print("\t数据库准备完毕, 一共插入了 %s 条数据, 可以进行搜索了! 一共耗时 %s 秒" % (counter,
                                                               (time.clock() - st), ) )
        

    def create_query(self):
        """生成一个Query对象, 并把引擎所绑定的Schema传给Query
        使得Query能够自行找到Schema中的各个Fields
        """
        return Query(self.schema)
    
    
    def search(self, query):
        """根据query进行单元搜索, 返回record tuple
        """
        main_sqlcmd_select_uuid, main_sqlcmd_select_all, keyword_sqlcmd_list = query.create_sql()

        ### 情况1, 主表和倒排索引表都要被查询
        if (len(keyword_sqlcmd_list) >= 1) and ("WHERE" in main_sqlcmd_select_uuid):
            # 得到查询主表所筛选出的 result_uuid_set
            result_uuid_set = OrderedSet([record[0] for record in self.engine.cursor.execute(main_sqlcmd_select_uuid)])

            # 得到使用倒排索引所筛选出的 keyword_uuid_set
            keyword_uuid_set = OrderedSet( set.intersection(
                *[self.engine.cursor.execute(sqlcmd).fetchone()[0] for sqlcmd in keyword_sqlcmd_list]
                ) )
            # 对两者求交集
            result_uuid_set = OrderedSet.intersection(result_uuid_set, keyword_uuid_set)
            # 根据结果中的uuid, 去主表中取数据
            for uuid in result_uuid_set:
                record = self.engine.cursor.execute("SELECT * FROM {0} WHERE {1} = {2}".format(self.schema.schema_name,
                                                                                               self.schema.uuid,
                                                                                               repr(uuid),) ).fetchone()
                yield record

        ### 情况2, 只对倒排索引表查询
        elif (len(keyword_sqlcmd_list) >= 1) and ("WHERE" not in main_sqlcmd_select_uuid):
            # 得到查询主表所筛选出的 result_uuid_set
            result_uuid_set = OrderedSet([record[0] for record in self.engine.cursor.execute(main_sqlcmd_select_uuid)])

            # 得到使用倒排索引所筛选出的 keyword_uuid_set
            keyword_uuid_set = OrderedSet( set.intersection(
                *[self.engine.cursor.execute(sqlcmd).fetchone()[0] for sqlcmd in keyword_sqlcmd_list]
                ) )
            # 对两者求交集
            result_uuid_set = OrderedSet.intersection(result_uuid_set, keyword_uuid_set)
            # 根据结果中的uuid, 去主表中取数据
            for uuid in result_uuid_set:
                record = self.engine.cursor.execute("SELECT * FROM {0} WHERE {1} = {2}".format(self.schema.schema_name,
                                                                                               self.schema.uuid,
                                                                                               repr(uuid),) ).fetchone()
                yield record
        
        ### 情况3, 只对主表查询
        elif (len(keyword_sqlcmd_list) == 0) and ("WHERE" in main_sqlcmd_select_uuid):
            for record in self.engine.cursor.execute(main_sqlcmd_select_all):
                yield record
        
        ### 情况4, 空查询
        else:
            pass

    def search_document(self, query):
        """根据query进行单元搜索, 返回document ordereddict
        example: OrderedDict({field_name: field_value})
        """
        counter = 0
        for record in self.search(query):
            counter += 1
            document = OrderedDict()
            # pack up as a ordered dict
            for field_name, field, value in zip(self.schema.fields.keys(), 
                                                self.schema.fields.values(), 
                                                record):    
                document[field_name] = value
            if counter <= query.limit_number:
                yield document
            else:
                return
            
    ### =================== 语法糖方法 =========================
    def _get_all_valid_keyword(self, field_name):
        """私有函数, 用于支持Engine.display_valid_keyword, Engine.search_valid_keyword功能
        根据field_name得到该field下所有出现过的keyword
        """
        if field_name in self.schema.keyword_fields:
            all_keywords = [row[0] for row in self.engine.execute("SELECT keyword FROM %s" % field_name)]
            return all_keywords
        else:
            raise Exception("ERROR! field_name has to be in %s, yours is %s" % (self.keyword_fields, 
                                                                                field_name))

    def _search_valid_keyword(self, field_name, pattern):
        """根据pattern, 搜索在单元名为field_name中可供选择的keyword"""
        result = [keyword for keyword in self._get_all_valid_keyword(field_name) if pattern in keyword]
        return result

    ### ================== Help ====================
    def print_as_table(self, array):
        for chunk in grouper(array, 5, ""):
            print("\t{0[0]:<20}\t{0[1]:<20}\t{0[2]:<20}\t{0[3]:<20}\t{0[4]:<20}".format(chunk) )
            
    def display_searchable_fields(self):
        """打印所有能被搜索到的单元名和具体类定义"""
        print("\n{:=^100}".format("All searchable fields"))
        for field_name, field in self.schema.fields.items():
            print("\t%s <---> %s" % (field_name, repr(field) ) )

    def display_keyword_fields(self):
        """打印所有支持倒排索引的单元名和具体类定义"""
        print("\n{:=^100}".format("All keyword fields"))
        for field_name, field in self.schema.fields.items():
            if "_Searchable_KEYWORD" in field.search_types:
                print("\t%s <---> %s" % (field_name, repr(field) ) )

    def display_criterion(self):
        """打印所有引擎支持的筛选条件"""
        query = self.create_query()
        print("\n{:=^100}".format("All supported criterion"))
        print("\t%s" % query.query_equal.help())
        print("\t%s" % query.query_greater.help())
        print("\t%s" % query.query_smaller.help())
        print("\t%s" % query.query_between.help())
        print("\t%s" % query.query_startwith.help())
        print("\t%s" % query.query_endwith.help())
        print("\t%s" % query.query_like.help())
        print("\t%s" % query.query_contains.help())

    def display_valid_keyword(self, field_name):
        """打印某个单元下所有有效的keyword的集合"""
        print("\n{:=^100}".format("All valid keyword in %s" % field_name))
        if field_name in self.schema.keyword_fields:
            all_keywords = self._get_all_valid_keyword(field_name)
            all_keywords.sort()
            self.print_as_table(all_keywords)
            print("Found %s valid keywords with in %s" % (len(all_keywords), 
                                                                     field_name) )
        else:
            print("ERROR! field_name has to be in %s, yours is %s" % (self.schema.keyword_fields, 
                                                                      field_name) )

    def search_valid_keyword(self, field_name, pattern):
        """根据pattern, 打印在单元名为field_name中可供选择的keyword"""
        print("\n{:=^100}".format("All valid keyword with pattern %s in %s" % (pattern,
                                                                               field_name) ) )
        result = self._search_valid_keyword(field_name, pattern)
        result.sort()
        self.print_as_table(result)
        print("Found %s valid keywords with pattern %s in %s" % (len(result), 
                                                                 pattern, 
                                                                 field_name))
        return result
    
    def help(self):
        """print help information"""
        text = \
        """
        Use the following command to help you create desired query:
        \tSearchEngine.display_searchable_fields()
        \tSearchEngine.display_keyword_fields()
        \tSearchEngine.display_criterion()
        \tSearchEngine.display_valid_keyword(field_name)
        \tSearchEngine.search_valid_keyword(field_name, pattern)
        """
        print(text)
        
##################################################
#                                                #
#                 Query class                    #
#                                                #
##################################################

class QueryEqual():
    def __init__(self, field_name, value):
        self.field_name = field_name
        self.value = value
        self.issql = True
        
    def __str__(self):
        return "%s = %s" % (self.field_name,
                            repr(self.value))
        
    @staticmethod
    def help():
        return "QueryEqual(field_name, value)"
        
class QueryGreater():
    def __init__(self, field_name, lowerbound):
        self.field_name = field_name
        self.lowerbound = lowerbound
        self.issql = True
        
    def __str__(self):
        return "%s >= %s" % (self.field_name,
                             repr(self.lowerbound),)
    
    @staticmethod
    def help():
        return "QueryGreater(field_name, lowerbound)"
    
class QuerySmaller():
    def __init__(self, field_name, upperbound):
        self.field_name = field_name
        self.upperbound = upperbound
        self.issql = True
    
    def __str__(self):
        return "%s <= %s" % (self.field_name,
                             repr(self.upperbound),)
    
    @staticmethod
    def help():
        return "QuerySmaller(field_name, upperbound)"
       
class QueryBetween():
    def __init__(self, field_name, lowerbound, upperbound):
        self.field_name = field_name
        self.lowerbound = lowerbound
        self.upperbound = upperbound
        self.issql = True
        
    def __str__(self):
        return "%s BETWEEN %s AND %s" % (self.field_name,
                                         repr(self.lowerbound),
                                         repr(self.upperbound),)
    
    @staticmethod
    def help():
        return "QueryBetween(field_name, lowerbound, upperbound)"
    
class QueryStartwith():
    def __init__(self, field_name, prefix):
        self.field_name = field_name
        self.prefix = prefix
        self.issql = True
    
    def __str__(self):
        return "{0} LIKE '{1}%'".format(self.field_name,
                                        self.prefix)
    
    @staticmethod
    def help():
        return "QueryStartwith(field_name, prefix)"
    
class QueryEndwith():
    def __init__(self, field_name, surfix):
        self.field_name = field_name
        self.surfix = surfix
        self.issql = True
    
    def __str__(self):
        return "{0} LIKE '%{1}'".format(self.field_name,
                                        self.surfix)
    
    @staticmethod
    def help():
        return "QueryEndwith(field_name, surfix)"
    
class QueryLike():
    def __init__(self, field_name, pattern):
        self.field_name = field_name
        self.pattern = pattern
        self.issql = True
    
    def __str__(self):
        return "{0} LIKE '%{1}%'".format(self.field_name,
                                         self.pattern)
    
    @staticmethod
    def help():
        return "QueryLike(field_name, pattern)"
    
class QueryContains():
    def __init__(self, field_name, *keywords):
        self.field_name = field_name
        self.subset = {keyword for keyword in keywords}
        self.issql = False
    
    @staticmethod
    def help():
        return "QueryContains(field_name, keyword1, keyword2, ...)"

class Query():
    """抽象查询对象, 用于储存用户自定义的查询条件
    讨论:
    
    在同时用sql语句以及倒排索引筛选时有两种生成result的做法:
    1. 先用倒排索引筛选出所有的uuid_set, 然后和用sql语句对主表进行查询所生成的uuid_set求交集。然后利用对
    主键查询的复杂度为1的特性, 按照uuid_set输出结果。
    2. 先用倒排索引筛选出所有的uuid_set, 然后依次检查主表查询的行结果, 如果行中的主键在uuid_set集合中, 
    则输出。
    经过试验证明, 无论什么情况, 方法1的速度都要远远优于方法2。
    """
    def __init__(self, schema):
        self.schema = schema
        self.criterions = list()
        
        self.orderby_clause = None
        self.limit_clause = None
        self.limit_number = 20
        self.offset_clause = None
        
        self.query_equal = QueryEqual
        self.query_greater = QueryGreater
        self.query_smaller = QuerySmaller
        self.query_between = QueryBetween
        self.query_startwith = QueryStartwith
        self.query_endwith = QueryEndwith
        self.query_like = QueryLike
        self.query_contains = QueryContains
        
    def add_criterion(self, criterion):
        """add a searching criterion 
        all criterions will be joint with AND operation by default.
        """
        if criterion.field_name in self.schema.fields:
            self.criterions.append(criterion)
        else:
            raise Exception("""criterion's field_name has to be a valid searchable fields.
            yours is = {0}
            all valid fields = {1}""".format(repr(criterion.field_name),
                                             list(self.schema.fields) ) )
        
    def add(self, criterion):
        """just a wrapper for Query.add_criterion(criterion)
        """
        self.add_criterion(criterion)
        
    def reset(self):
        """clear all plugged in criterions
        """
        self.criterions.clear()
        
    def renew_with(self, *argv):
        """clear all, and plug in many new criterions
        """
        self.reset()
        for criterion in argv:
            self.add(criterion)

    def order_by(self, fieldname_list, order_list):
        """ORDER BY Country DESC, CustomerName ASC;
        example:
            fieldname_list = ["column_name1", "column_name2"]
            order_list = ["DESC", "ASC"]
        """
        if (len(fieldname_list) == len(order_list) ) and (len(fieldname_list) >= 1): # exam input
            try: # exam whether field name is valid
                for text in fieldname_list:
                    if text not in self.schema.fields:
                        raise Exception("'%s' doens't match any field name in %s!" % (text, 
                                                                                      list(self.schema.fields.keys())))
            except Exception as e:
                raise e
            try: # exam order keyword is DESC or ASC
                for text in order_list:
                    if text.upper() not in {"DESC", "ASC"}:
                        raise Exception("order_list has to be 'DESC' or 'ASC'! (not case sensitive)")
            except Exception as e:
                raise e
            # define orderby_clause
            self.orderby_clause = "ORDER BY " + \
                ", ".join(["%s %s" % (fieldname, order) for fieldname, order in zip(fieldname_list, order_list)])
        else:
            raise Exception("%s doens't match %s!" % (fieldname_list, order_list))
        
    def limit(self, howmany):
        self.limit_clause = "LIMIT %s" % howmany
        self.limit_number = howmany
        
    def offset(self, howmany):
        self.offset_clause = "OFFSET %s" % howmany

    def _split_SqlCriterions_and_KeywordCriterions(self):
        """分离对主表查询的criterion和对倒排索引表查询的criterion
        returns
        -------
            sql_criterions: criterion that apply to the main table
            keyword_criterions: criterion that apply to the invert index 
        """
        sql_criterions = list()
        keyword_criterions = list()
        for criterion in self.criterions:
            if criterion.issql:
                sql_criterions.append(criterion)
            else:
                keyword_criterions.append(criterion)
        return sql_criterions, keyword_criterions
 
    def create_sql(self):
        """generate SELECT SQL command for searchengine
        """
        sql_criterions, keyword_criterions = self._split_SqlCriterions_and_KeywordCriterions()
        
        ### SQL command for the main table (which table_name = Engine.schema_name)
        select_uuid_clause = "SELECT %s FROM %s" % (self.schema.uuid, self.schema.schema_name)
        select_all_clause = "SELECT * FROM %s" % self.schema.schema_name
        if len(sql_criterions) >= 1:
            where_clause = "WHERE %s" % " AND ".join([str(criterion) for criterion in sql_criterions])
        else:
            where_clause = ""

        main_sqlcmd_select_uuid = "\n\t".join([i for i in [select_uuid_clause,
                                                           where_clause,
                                                           self.orderby_clause,
                                                           self.offset_clause] if i ])  

        main_sqlcmd_select_all = "\n\t".join([i for i in [select_all_clause,
                                                          where_clause,
                                                          self.orderby_clause,
                                                          self.offset_clause] if i ])  
        ### SQL command for the invert index table (which table_name = Engine.keyword_fields)
        keyword_sqlcmd_list = list()
        for criterion in keyword_criterions:
            for keyword in criterion.subset:
                select_clause = "SELECT\t{0}\n".format("uuid_set")
                from_clause = "FROM\t{0}\n".format(criterion.field_name)
                where_clause = "WHERE\t{0} = {1}".format("keyword", repr(keyword))
                keyword_sqlcmd_list.append(select_clause + from_clause + where_clause)
        
        return main_sqlcmd_select_uuid, main_sqlcmd_select_all, keyword_sqlcmd_list
    