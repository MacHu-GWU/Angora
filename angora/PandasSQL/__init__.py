##encoding=UTF8

"""
数据库 到 数据库 操作可以抽象为下面的代码
for row in engine1.select(table1):
    dict = row.to_dict()
    newdict = convert(newdict)
    engine2.insert(table2, newdict)
    
csv 到 数据库 操作可以抽象为下面的代码
for row in csv.read():
    dict = row.to_dict()
    newdict = convert(newdict)
    engine.insert(table, newdict)
    
数据库 到 csv 操作可以抽象为下面的代码
for row in engine.select(table):
    dict = row.to_dict()
    newdict = convert(newdict)
    csv.append(newdict)
csv.dump()

所以我们所需要的中间层应该是有如下几个东西:

1. 二维抽象表, 包括表名, 列名, 列数据格式。
2. 数据条目row, 类字典的对象。
3. 转换器, 用于将数据源中的数据转换成目标数据
4. 匹配器, 用于根据数据源的定义和目标数据的定义, 对列, 数据格式进行匹配

所以整个过程可以抽象为:
    1. 从数据源拿出一条数据
    2. 用匹配器去分析匹配, 将结果告诉转换器
    3. 从数据源取出数据, 用转换器转换
    4. 放入目标数据库
"""

from .sqlite3blackhole import Sqlite3BlackHole, CSVFile