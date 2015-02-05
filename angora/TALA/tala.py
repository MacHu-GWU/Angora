##encoding=utf8

"""
author: Sanhe Hu

compatibility: python3 ONLY

prerequisites: angora.SQLITE

import:
    from angora.TALA.tala import FieldType, Field, Schema, SearchEngine
"""

from __future__ import print_function
from angora.SQLITE.core import Row, Select, DataType, MetaData, Column, Table, Sqlite3Engine
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
    sqlite_dtype_name = "Date"
    is_pickletype = False
    
class _Searchable_DATETIME(SEARCHABLE_TYPE):
    """日期时间逻辑匹配"""
    name = "_Searchable_DATETIME"
    sqlite_dtype = datatype.datetime
    sqlite_dtype_name = "DateTime"
    is_pickletype = False
    
class _Searchable_INTEGER(SEARCHABLE_TYPE):
    """整数逻辑匹配"""
    name = "_Searchable_INTEGER"
    sqlite_dtype = datatype.integer
    sqlite_dtype_name = "Integer"
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
    sqlite_dtype_name = "PickleType"
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
            self.engine.insert_row(ins, row)
            
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
        main_sqlcmd, main_sqlcmd_select_all, keyword_sqlcmd_list = query.create_sql()
        
        ### 情况1, 主表和倒排索引表都要被查询
        if (len(keyword_sqlcmd_list) >= 1) and ("WHERE" in main_sqlcmd):
            # 得到查询主表所筛选出的 result_uuid_set
            result_uuid_set = {record[0] for record in self.engine.cursor.execute(main_sqlcmd) }

            # 得到使用倒排索引所筛选出的 keyword_uuid_set
            keyword_uuid_set = set.intersection(
                *[self.engine.cursor.execute(sqlcmd).fetchone()[0] for sqlcmd in keyword_sqlcmd_list]
                )
            # 对两者求交集
            result_uuid_set.intersection_update(keyword_uuid_set)
            # 根据结果中的uuid, 去主表中取数据
            for uuid in result_uuid_set:
                record = self.engine.cursor.execute("SELECT * FROM {0} WHERE {1} = {2}".format(self.schema.schema_name,
                                                                                               self.schema.uuid,
                                                                                               repr(uuid),) ).fetchone()
                yield record
        
        ### 情况2, 只对倒排索引表查询
        elif (len(keyword_sqlcmd_list) >= 1) and ("WHERE" not in main_sqlcmd):
            keyword_uuid_set = set.intersection(
                *[self.engine.cursor.execute(sqlcmd).fetchone()[0] for sqlcmd in keyword_sqlcmd_list]
                )
            for uuid in keyword_uuid_set:
                record = self.engine.cursor.execute("SELECT * FROM {0} WHERE {1} = {2}".format(self.schema.schema_name,
                                                                                               self.schema.uuid,
                                                                                               repr(uuid),) ).fetchone()
                yield record
        
        ### 情况3, 只对主表查询
        elif (len(keyword_sqlcmd_list) == 0) and ("WHERE" in main_sqlcmd):
            for record in self.engine.cursor.execute(main_sqlcmd_select_all):
                yield record
        
        ### 情况4, 空查询
        else:
            pass

    def search_document(self, query):
        """根据query进行单元搜索, 返回document ordereddict
        example: OrderedDict({field_name: field_value})
        """
        for record in self.search(query):
            document = OrderedDict()
            # pack up as a ordered dict
            for field_name, field, value in zip(self.schema.fields.keys(), 
                                                self.schema.fields.values(), 
                                                record):    
                document[field_name] = value
            yield document

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
        
class QueryGreater():
    def __init__(self, field_name, lowerbound):
        self.field_name = field_name
        self.lowerbound = lowerbound
        self.issql = True
        
    def __str__(self):
        return "%s >= %s" % (self.field_name,
                             repr(self.lowerbound),)
        
class QuerySmaller():
    def __init__(self, field_name, upperbound):
        self.field_name = field_name
        self.upperbound = upperbound
        self.issql = True
    
    def __str__(self):
        return "%s <= %s" % (self.field_name,
                             repr(self.upperbound),)
            
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

class QueryStartwith():
    def __init__(self, field_name, prefix):
        self.field_name = field_name
        self.prefix = prefix
        self.issql = True
    
    def __str__(self):
        return "{0} LIKE '{1}%'".format(self.field_name,
                                        self.prefix)
        
class QueryEndwith():
    def __init__(self, field_name, surfix):
        self.field_name = field_name
        self.surfix = surfix
        self.issql = True
    
    def __str__(self):
        return "{0} LIKE '%{1}'".format(self.field_name,
                                        self.surfix)

class QueryLike():
    def __init__(self, field_name, piece):
        self.field_name = field_name
        self.piece = piece
        self.issql = True
    
    def __str__(self):
        return "{0} LIKE '%{1}%'".format(self.field_name,
                                         self.piece)

class QueryContains():
    def __init__(self, field_name, *keywords):
        self.field_name = field_name
        self.subset = {keyword for keyword in keywords}
        self.issql = False

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
            raise Exception("""criterion has to be applied to one of the searchable fields.
            yours is = {0}
            all valid fields = {1}""".format(repr(criterion.field_name),
                                             list(self.schema.fields) ) )
        
    def add(self, criterion):
        """just a wrapper for Query.add_criterion(criterion)
        """
        self.add_criterion(criterion)
        
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
        """生成对主表进行查询的SQL语句和若干个对倒排索引表查询的SQL语句
        create one SQL command for the main_table and create several SQL command for invert index
        table
        """
        sql_criterions, keyword_criterions = self._split_SqlCriterions_and_KeywordCriterions()
        
        ### SQL command for the main table (which table_name = Engine.schema_name)
        select_clause = "SELECT\t{0}\n".format(self.schema.uuid)
        select_clause_all = "SELECT\t*\n"
        
        from_clause = "FROM\t{0}\n".format(self.schema.schema_name)
        if len(sql_criterions) >= 1:
            where_clause = "WHERE\t" + "\n\tAND ".join([str(criterion) for criterion in sql_criterions])
        else:
            where_clause = ""
        main_sqlcmd = select_clause + from_clause + where_clause
        main_sqlcmd_select_all = select_clause_all + from_clause + where_clause
        ### SQL command for the invert index table (which table_name = Engine.keyword_fields)
        keyword_sqlcmd_list = list()
        for criterion in keyword_criterions:
            for keyword in criterion.subset:
                select_clause = "SELECT\t{0}\n".format("uuid_set")
                from_clause = "FROM\t{0}\n".format(criterion.field_name)
                where_clause = "WHERE\t{0} = {1}".format("keyword", repr(keyword))
                keyword_sqlcmd_list.append(select_clause + from_clause + where_clause)
        
        return main_sqlcmd, main_sqlcmd_select_all, keyword_sqlcmd_list
