##encoding=utf8

"""
Table

Columns
"""

from .wrapper import iterC
from collections import OrderedDict
import sqlite3
import pickle

def obj2bytestr(obj):
    """convert arbitrary object to database friendly bytestr"""
    return pickle.dumps(obj)

def bytestr2obj(bytestr):
    """recovery object from bytestr"""
    return pickle.loads(bytestr)

def bytestr2hexstring(bytestr):
    """convert byte string to hex string, for example
    b'\x80\x03]q\x00(K\x01K\x02K\x03e.'  to  X'80035d7100284b014b024b03652e'
    """
    res = list()
    for i in bytestr:
        res.append(str(hex(i))[2:].zfill(2))
    return "".join(res)

### === Row class ===
class Row():
    """
    """
    def __init__(self, columns, values):
        self.columns = columns
        self.values = values
        self.dictionary_view = None
        
    def _create_dict_view(self):
        self.dictionary_view = OrderedDict()
        for i, j in zip(self.columns, self.values):
            self.dictionary_view[i] = j
            
    def __str__(self):
        if not self.dictionary_view:
            self._create_dict_view()
        return str(self.dictionary_view)
    
    def __repr__(self):
        return "Row(columns=%s, values=%s)" % (self.columns, self.values)
    
    def __getitem__(self, key):
        if not self.dictionary_view:
            self._create_dict_view()
        return self.dictionary_view[key]
    
    def __getattr__(self, attr):
        if not self.dictionary_view:
            self._create_dict_view()
        return self.dictionary_view[attr]

### === Insert class ===
class Insert():
    def __init__(self, table):
        self.table = table
        if len(table.pickletype_columns) == 0: # Define default record converter
            self.default_record_converter = self.nonpicklize_record
            self.default_row_converter = self.nonpicklize_row
        else:
            self.default_record_converter = self.picklize_record
            self.default_row_converter = self.picklize_row
            
    def sqlcmd_from_record(self):
        """generate the 'INSERT INTO table...' SQL command suit for record, for example:
        INSERT INTO table_name VALUES (?,?,...,?);
        """
        cmd_INSERT_INTO = "INSERT INTO %s" % self.table.table_name
        cmd_COLUMNS = "(%s)" % ", ".join(self.table.columns)
        cmd_KEYWORD_VALUES = "VALUES"
        cmd_QUESTION_MARK = "(%s)" % ", ".join(["?"] * len(self.table.columns) )
        template = "%s\n\t%s\n%s\n\t%s;"
        self.insert_sqlcmd = template % (cmd_INSERT_INTO,
                                         cmd_COLUMNS,
                                         cmd_KEYWORD_VALUES,
                                         cmd_QUESTION_MARK,)
        
    def sqlcmd_from_row(self, row):
        """generate the 'INSERT INTO table...' SQL command suit for row, for example:
        INSERT INTO table_name (column1, column2, ..., columnN) VALUES (?,?,...,?);
        """
        cmd_INSERT_INTO = "INSERT INTO %s" % self.table.table_name
        cmd_COLUMNS = "(%s)" % ", ".join(row.columns)
        cmd_KEYWORD_VALUES = "VALUES"
        cmd_QUESTION_MARK = "(%s)" % ", ".join(["?"] * len(row.columns) )
        template = "%s\n\t%s\n%s\n\t%s;"
        self.insert_sqlcmd = template % (cmd_INSERT_INTO,
                                         cmd_COLUMNS,
                                         cmd_KEYWORD_VALUES,
                                         cmd_QUESTION_MARK,)
    
    ### record/row converter to change the record/row to sqlite3 friendly tuple
    def nonpicklize_record(self, record):
        """把一个不含PICKLETYPE的record原样返回
        """
        return record
            
    def picklize_record(self, record):
        """把一个含有PICKLETYPE的record中的pickle对应列转化成pickle string再返回
        """
        new_record = list()
        for column, item in zip(self.table.columns.values(), record):
            if column.is_pickletype:
                new_record.append(obj2bytestr(item))
            else:
                new_record.append(item)
        return tuple(new_record)

    def nonpicklize_row(self, row):
        """把一个不含PICKLETYPE的row其中的values, 即行的tuple原样返回
        """
        return row.values

    def picklize_row(self, row):
        """把一个含有PICKLETYPE的row中的pickle对应列转化成pickle string再封装成tuple返回
        """
        new_record = list()
        for column, item in zip(row.columns, row.values):
            if self.table.columns[column].is_pickletype:
                new_record.append(obj2bytestr(item))
            else:
                new_record.append(item)
        return tuple(new_record)

### === Update class ===
class _Update_config():
    """用于判断每个列上的值是绝对更新还是相对更新
    """
    def __init__(self, sqlcmd):
        self.sqlcmd = sqlcmd

class Update():
    def __init__(self, table):
        self.table = table
        self.update_clause = "UPDATE %s" % self.table.table_name
        self.where_clause = None
        
    def values(self, **kwarg):
        """
        SET
            movie.length = movie.length + 9999, # 相对更新
            movie.title ="ABCD", # 绝对更新
            movie.genres = 'gANjYnVpbHRpbnMKc2V0CnEAXXEBWAkAAABBZHZlbnR1cmVxAmGFcQNScQQu', # 绝对更新
            movie.release_date = '1500-01-01' # 绝对更新
        """
        res = list()
        for column_name, value in kwarg.items():
            column = self.table.columns[column_name]
            try: # 处理相对更新
                res.append("%s = %s" % (column.column_name, 
                                        value.sqlcmd ) ) # 直接使用
            except: # 处理绝对更新
                res.append("%s = %s" % (column.column_name, 
                                        column.__SQL__(value) ) ) # 将 = value 中 value 的部分处理
                
        self.set_clause = "SET\n\t%s" % ",\n\t".join(res)
        return self
    
    def where(self, *argv):
        self.where_clause = "WHERE\n\t%s" % " AND\n\t".join(argv)
        return self
    
    def sqlcmd(self):
        """generate "UPDATE table SET..." SQL command
        
        example output:
        
            UPDATE movie
            SET
                genres = 'gANjYnVpbHRpbnMKc2V0CnEAXXEBWAkAAABBZHZlbnR1cmVxAmGFcQNScQQu',
                length = length + 9999,
                release_date = '1500-01-01',
                title = 'ABCDEFG'
            WHERE
                release_date <= '2000-01-01' AND
                length > 100
        """
        self.update_sqlcmd = "\n".join([i for i in [self.update_clause, 
                                                   self.set_clause, 
                                                   self.where_clause] if i])

### === Select class ===
class Select():
    def __init__(self, columns):
        self.columns = columns
        self.column_names = tuple([column.column_name for column in self.columns])
        
        self.select_from_clause = "SELECT %s FROM %s" % (", ".join(self.column_names), 
                                                         self.columns[0].table_name)
        self.where_clause = None
        self.limit_clause = None
        
        # Define default record converter, convert pickletype byte string back to python object
        if len([column for column in self.columns if column.is_pickletype]) == 0: 
            self.default_record_converter = self.nonpicklize_record
        else:
            self.default_record_converter = self.picklize_record

    def where(self, *argv):
        self.where_clause = "WHERE %s" % " AND ".join(argv)
        return self
    
    def limit(self, howmany):
        self.limit_clause = "LIMIT %s" % howmany
        return self
    
    def __str__(self):
        return "\n\t".join([i for i in [self.select_from_clause,
                                        self.where_clause,
                                        self.limit_clause] if i ])
    
    def toSQL(self):
        """return the SELECT SQL command"""
        return str(self)

    ### record converter to change the record to sqlite3 friendly tuple
    def nonpicklize_record(self, record):
        """把一个不含PICKLETYPE的record原样返回
        """
        return record
            
    def picklize_record(self, record):
        """把一个含有PICKLETYPE的record中的pickle对应列转化成pickle string再返回
        """
        new_record = list()
        for column, item in zip(self.columns, record):
            if column.is_pickletype:
                new_record.append(bytestr2obj(item))
            else:
                new_record.append(item)
        return tuple(new_record)

### === MetaData class ===
class MetaData():
    def __init__(self, bind=None):
        self.bind = bind
        self.tables = dict()

    def create_all(self, engine):
        for table in self.tables.values():
            create_table_sqlcmd = table.create_table_sql()
            try:
                engine.cursor.execute(create_table_sqlcmd)
            except Exception as e:
                print(e)
            
    def reflect(self, engine):
        import sqlalchemy
        SAengine = sqlalchemy.create_engine("sqlite:///%s" % engine.dbname, echo=False)
        meta = sqlalchemy.MetaData()
        meta.reflect(bind=SAengine)
        
        dtype_mapping = {"TEXT": TEXT(), "INTEGER": INTEGER(), "REAL": REAL(), "DATE": DATE(),
                         "DATETIME": DATETIME(), "BLOB": PICKLETYPE(),}
        
        for table in meta.tables.values():
            
            columns = list()
            
            for column in table.columns.values():
                if column.server_default:
                    columns.append(Column(column.name, 
                                          dtype_mapping[str(column.type)],
                                          primary_key=column.primary_key,
                                          nullable=column.nullable,
                                          default=eval(column.server_default.arg.text),))
                else:
                    columns.append(Column(column.name, 
                                          dtype_mapping[str(column.type)],
                                          primary_key=column.primary_key,
                                          nullable=column.nullable,))
            table = Table(table.name, self, *columns)

### === DataType class ===
class BaseDataType():
    """所有数据类型的父类
    """
    def __str__(self):
        return self.name
    
    def __repr__(self):
        return "%s()" % self.name
    
class TEXT(BaseDataType):
    name = "TEXT"
    sqlite_dtype_name = "TEXT"

class INTEGER(BaseDataType):
    name = "INTEGER"
    sqlite_dtype_name = "INTEGER"
    
class REAL(BaseDataType):
    name = "REAL"
    sqlite_dtype_name = "REAL"
    
class DATE(BaseDataType):
    name = "DATE"
    sqlite_dtype_name = "DATE"
    
class DATETIME(BaseDataType):
    name = "DATETIME"
    sqlite_dtype_name = "DATETIME"
    
class PYTHONLIST(BaseDataType):
    name = "PYTHONLIST"
    sqlite_dtype_name = "PYTHONLIST"
    
class PYTHONSET(BaseDataType):
    name = "PYTHONSET"
    sqlite_dtype_name = "PYTHONSET"
   
class PYTHONDICT(BaseDataType):
    name = "PYTHONDICT"
    sqlite_dtype_name = "PYTHONDICT"
    
class ORDEREDDICT(BaseDataType):
    name = "ORDEREDDICT"
    sqlite_dtype_name = "ORDEREDDICT"
    
class PICKLETYPE(BaseDataType):
    name = "PICKLETYPE"
    sqlite_dtype_name = "BLOB"
    
class DataType():
    """数据类型的容器类, 用于通过DataType.text来调用TEXT(), 可以解决与其他库里面同样需要
    import TEXT, INTEGER, ... 然后发生命名空间冲突的问题。
    """
    text = TEXT()
    integer = INTEGER()
    real = REAL()
    date = DATE()
    datetime = DATETIME()
    pythonlist = PYTHONLIST()
    pythonset = PYTHONSET()
    pythondict = PYTHONDICT()
    pickletype = PICKLETYPE()

### === DataType sqlite3 converter ===
def adapt_list(_LIST):
    """类 -> 字符串 转换"""
    return obj2bytestr(_LIST)

def convert_list(_STRING):
    """字符串 -> 类 转换"""
    return bytestr2obj(_STRING)

def adapt_set(_SET):
    """类 -> 字符串 转换"""
    return obj2bytestr(_SET)

def convert_set(_STRING):
    """字符串 -> 类 转换"""
    return bytestr2obj(_STRING)

def adapt_dict(_DICT):
    """类 -> 字符串 转换"""
    return obj2bytestr(_DICT)

def convert_dict(_STRING):
    """字符串 -> 类 转换"""
    return bytestr2obj(_STRING)

def adapt_ordereddict(_ORDEREDDICT):
    """类 -> 字符串 转换"""
    return obj2bytestr(_ORDEREDDICT)

def convert_ordereddict(_STRING):
    """字符串 -> 类 转换"""
    return bytestr2obj(_STRING)

### === Column class ===
class Column():
    def __init__(self, column_name, data_type, primary_key=False, nullable=True, default=None):
        if column_name in ["table_name", "columns", "primary_key_columns", "pickletype_columns", "all"]:
            raise Exception("""column name cannot use those system reserved name:
            "table_name", "columns", "primary_key_columns", "pickletype_columns", "all";""")
        
        self.column_name = column_name
        self.table_name = None
        self.full_name = None
        self.data_type = data_type
        
        # 判断列的名称
        self.is_date = self.data_type.name == "DATE"
        self.is_datetime = self.data_type.name == "DATETIME"
        self.is_pickletype = self.data_type.name == "PICKLETYPE"
        
        # 根据数据类型, 绑定将数值转换成在SQL语句中说显示的字符串的转换器
        if self.is_date:
            self.__SQL__ = self._sql_DATE
        elif self.is_datetime:
            self.__SQL__ = self._sql_DATETIME
        elif self.is_pickletype:
            self.__SQL__ = self._sql_PICKLETYPE
        else:
            self.__SQL__ = self._sql_STRING_NUMBER
        
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default
                    
    def __str__(self):
        """return column_name
        """
        return self.column_name

    def __repr__(self):
        """return the string represent the Column object that can recover the object from it
        """
        return "Column('%s', %s, primary_key=%s, nullable=%s, default=%s)" % (self.column_name,
                                                                              repr(self.data_type),
                                                                              self.primary_key,
                                                                              self.nullable,
                                                                              repr(self.default),)

    def _sql_STRING_NUMBER(self, value):
        """if it is string or number, in sql command it's just 'string', number
        """
        return repr(value)

    def _sql_DATE(self, value):
        """if it is datetime.date object, in sql command it's just '1999-01-01'
        """
        return repr(str(value))
    
    def _sql_DATETIME(self, value):
        """if it is datetime.datetime object, in sql command it's just '1999-01-01 06:30:00'
        """
        return repr(str(value)[:19])

    def _sql_PICKLETYPE(self, value):
        """if it is python object, in sql command we convert it to byte string, like 'gx4=fjl82d...'
        """
        return "X'%s'" % bytestr2hexstring(obj2bytestr(value))
    
    def create_table_sql(self):
        """generate the definition part of 'CREATE TABLE (...)' SQL command
        by column name, data type, constrains.
        
        example output:
        
            movie_id TEXT
            title TEXT DEFAULT 'unknown_title'
            length INTEGER DEFAULT -1
            release_date DATE DEFAULT '0000-01-01'
            genres TEXT DEFAULT 'gANjYnVpbHRpbnMKc2V0CnEAXXEBhXECUnEDLg=='
        """
        name_part = self.column_name
        dtype_part = self.data_type.sqlite_dtype_name
        if self.nullable == True:
            nullable_part = None
        else:
            nullable_part = "NOT NULL"
        if self.default == None:
            default_part = None
        else:
            default_part = "DEFAULT %s" % self.__SQL__(self.default)
        return " ".join([i for i in [name_part, dtype_part, nullable_part, default_part] if i])
    
    ## for Select().where() method. example Select().where(column_name >= 100)
    
    def __lt__(self, other):
        return "%s < %s" % (self.column_name, self.__SQL__(other))

    def __le__(self, other):
        return "%s <= %s" % (self.column_name, self.__SQL__(other))
    
    def __eq__(self, other):
        return "%s = %s" % (self.column_name, self.__SQL__(other))
    
    def __ne__(self, other):
        return "%s != %s" % (self.column_name, self.__SQL__(other))
    
    def __gt__(self, other):
        return "%s > %s" % (self.column_name, self.__SQL__(other))
    
    def __ge__(self, other):
        return "%s >= %s" % (self.column_name, self.__SQL__(other))
    
    ## for Update().values() method. example: Update.values(column_name = column_name + 100)
    
    def __add__(self, other):
        return _Update_config("%s + %s" % (self.column_name, self.__SQL__(other)) )
    
    def __sub__(self, other):
        return _Update_config("%s - %s" % (self.column_name, self.__SQL__(other)) )
    
    def __mul__(self, other):
        return _Update_config("%s * %s" % (self.column_name, self.__SQL__(other)) )
    
    def __div__(self, other):
        return _Update_config("%s / %s" % (self.column_name, self.__SQL__(other)) )
    
    def __pos__(self):
        return _Update_config("- %s" % self.column_name)
    
    def __neg__(self):
        return _Update_config("+ %s" % self.column_name)
    
### === Table class ===
class Table(): 
    def __init__(self, table_name, metadata, *args):
        self.table_name = table_name
        self.columns = OrderedDict()
        self.primary_key_columns = list()
        self.pickletype_columns = list()
        
        for column in args:
            column.table_name = self.table_name
            column.full_name = "%s.%s" % (self.table_name, column.column_name)
            self.columns[column.column_name] = column
            if column.primary_key:
                self.primary_key_columns.append(column.column_name)
            if column.is_pickletype:
                self.pickletype_columns.append(column.column_name)
        
        self.all = list(self.columns.values() );
                
        metadata.tables[self.table_name] = self
        
    def __str__(self):
        return self.table_name
    
    def __repr__(self):
        return "Table('%s', MetaData(bind=None), %s)" % (self.table_name, 
                                                         ", ".join([repr(column) for column in self.columns.values()]))
    
    def __getattr__(self, attr):
        if attr in self.columns:
            return self.columns[attr]
        else:
            raise AttributeError
    
    def create_table_sql(self):
        """generate the 'CREATE TABLE...' SQL command
        
        example output:
        
            CREATE TABLE movie (
                movie_id TEXT,
                title TEXT DEFAULT 'unknown_title',
                length INTEGER DEFAULT -1,
                release_date DATE DEFAULT '0000-01-01',
                genres TEXT DEFAULT 'gANjYnVpbHRpbnMKc2V0CnEAXXEBhXECUnEDLg==',
                PRIMARY KEY (movie_id));
        """
        cmd_CREATE_TABLE = "CREATE TABLE %s" % self.table_name
        cmd_DATATYPE = ",\n\t".join([column.create_table_sql() for column in self.columns.values()])
        
        primary_key_columns = [column.column_name for column in self.columns.values() if column.primary_key]
        if len(primary_key_columns) == 0:
            cmd_PRIMARY_KEY = ""
        else:
            cmd_PRIMARY_KEY = ",\n\tPRIMARY KEY (%s)" % ", ".join(primary_key_columns)
            
        template = "%s (\n\t%s%s);" 
        return template % (cmd_CREATE_TABLE,
                           cmd_DATATYPE,
                           cmd_PRIMARY_KEY,)
    
    def insert(self):
        """create a Insert object
        """
        return Insert(self)

    def update(self):
        """create a Update object
        """
        return Update(self)

### === Sqlite3Engine class ===
class Sqlite3Engine():
    def __init__(self, dbname, autocommit=True):
        self.dbname = dbname

        sqlite3.register_adapter(list, adapt_list)
        sqlite3.register_converter("PYTHONLIST", convert_list)
        sqlite3.register_adapter(set, adapt_set)
        sqlite3.register_converter("PYTHONSET", convert_set)
        sqlite3.register_adapter(dict, adapt_dict)
        sqlite3.register_converter("PYTHONDICT", convert_dict)
        sqlite3.register_adapter(OrderedDict, adapt_ordereddict)
        sqlite3.register_converter("ORDEREDDICT", convert_ordereddict)
        
        self.connect = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
        self.cursor = self.connect.cursor()
        
        if self.autocommit:
            self._commit = self.commit
        else:
            self._commit = self.commit_nothing
    
    def autocommit(self, flag):
        if flag:
            self._commit = self.commit
        else:
            self._commit = self.commit_nothing
            
    ### === Insert ===
    def insert_record(self, insert_obj, record):
        """插入单条记录"""
        insert_obj.sqlcmd_from_record()
        self.cursor.execute(insert_obj.insert_sqlcmd, insert_obj.default_record_converter(record))
        self._commit()

    def insert_many_records(self, insert_obj, records):
        """插入多条记录"""
        insert_obj.sqlcmd_from_record()
        for record in records:
            try:
                self.cursor.execute(insert_obj.insert_sqlcmd, insert_obj.default_record_converter(record))
            except:
                pass
        self._commit()
        
    def insert_row(self, insert_obj, row):
        """插入单条Row object"""
        insert_obj.sqlcmd_from_row(row)
        self.cursor.execute(insert_obj.insert_sqlcmd, insert_obj.default_row_converter(row))
        self.connect.commit()
    
    def _insert_many_rows_list_mode(self, insert_obj, rows):
        """内部函数, 以列表模式插入多条Row object"""
        row = rows[0]
        insert_obj.sqlcmd_from_row(row)
        if set(row.columns).isdisjoint(set(insert_obj.table.pickletype_columns)): # 如果没有交集
            insert_obj.current_converter = insert_obj.nonpicklize_row
        else: # 如果有交集, 要用到picklize_row
            insert_obj.current_converter = insert_obj.picklize_row
        
        for row in rows:
            try:
                self.cursor.execute(insert_obj.insert_sqlcmd, insert_obj.current_converter(row))
            except:
                pass
        self._commit()
                
    def _insert_many_rows_generator_mode(self, insert_obj, rows):
        """内部函数, 以生成器模式插入多条Row object"""
        row = next(rows)
        insert_obj.sqlcmd_from_row(row)
        if set(row.columns).isdisjoint(set(insert_obj.table.pickletype_columns)): # 如果没有交集
            insert_obj.current_converter = insert_obj.nonpicklize_row
        else: # 如果有交集, 要用到picklize_row
            insert_obj.current_converter = insert_obj.picklize_row
            
        try: # manually insert the first element of the generator
            self.cursor.execute(insert_obj.insert_sqlcmd, insert_obj.current_converter(row))
        except:
            pass
        for row in rows:
            try:
                self.cursor.execute(insert_obj.insert_sqlcmd, insert_obj.current_converter(row))
            except:
                pass
        self._commit()

    def insert_many_rows(self, insert_obj, rows):
        """自动判断以生成器模式还是列表模式插入多条Row object
        """
        try:
            self._insert_many_rows_generator_mode(insert_obj, rows)
        except TypeError:
            self._insert_many_rows_list_mode(insert_obj, rows)
    
    def insert_and_update_many_records(self, insert_obj, records):
        update_obj = insert_obj.table.update()
        insert_obj.sqlcmd_from_record()
        
        for record in records: # try insert one by one
            try: # try insert
                self.cursor.execute(insert_obj.insert_sqlcmd, 
                                    insert_obj.default_record_converter(record))
            except sqlite3.IntegrityError: # 
                values_kwarg = dict() # update.values()'s argument
                where_args = list() # update.values().where()'s argument
                 
                for column_name, column, value in zip(update_obj.table.columns,
                                                      update_obj.table.columns.values(),
                                                      record):
                    values_kwarg[column_name] = value # fill in values dictionary
                    if column.primary_key: # use primary_key value to locate the row to update
                        where_args.append( column == value)
                # update one
                self.update( update_obj.values(**values_kwarg).where(*where_args) )
                
        self._commit()
        
    ### === Select ===
    def select(self, select_obj):
        """以生成器形式返回行数据
        """
        for record in self.cursor.execute(select_obj.toSQL()):
            yield select_obj.default_record_converter(record)
            
    def select_row(self, select_obj):
        """以生成器形式返回封装成Row对象的行数据
        """
        for record in self.select(select_obj):
            row = Row(select_obj.column_names, record)
            yield row
    
    ### === Update ===
    def update(self, update_obj):
        update_obj.sqlcmd()
        self.cursor.execute(update_obj.update_sqlcmd)
        self.connect.commit()
    
    ### === 一些简便的语法糖函数 ===
    def select_all(self, table):
        """选择整个表
        """
        self.cursor.execute("SELECT * FROM %s;" % table.table_name)
        return iterC(self.cursor)
    
    def prt_all(self, table):
        """打印整个表
        """
        self.cursor.execute("SELECT * FROM %s;" % table.table_name)
        counter = 0
        for record in iterC(self.cursor):
            print(record)
            counter += 1
        print("Found %s records in %s" % (counter, table.table_name))
    
    def prt_howmany(self, table):
        """打印表内有多少条记录
        """
        self.cursor.execute("SELECT COUNT(*) FROM (SELECT * FROM %s);" % table.table_name)
        print("Found %s records in %s" % (self.cursor.fetchone()[0], table.table_name))
        
    def execute(self, *args, **kwarg):
        return self.cursor.execute(*args, **kwarg)
        
    def commit(self):
        """method for manually commit operation
        """
        self.connect.commit()

    def commit_nothing(self):
        """method for doing nothing
        """
        pass
    
