#encoding=utf-8

from fuzzywuzzy import process

class Sheet():
    def __init__(self, dtype):
        """定义我们数据的目标格式
        """
        self.dtype = dtype
        self.supported_dtype = {
            "TEXT": {"str"},
            "INTEGER": {"int", "int8", "int16", "int32", "int64"},
            "REAL": {"float", "float16", "float32", "float64"},
            "DATE": {"date"},
            "DATETIME": {"datetime"},
            }
        
    def match(self, row, criterion=0):
        """根据dtype的定义, 自动从数据源row中匹配column - key
        1. 首先根据column_name和key_name匹配, 优先挑选匹配度高的
        2. 然后检查数据类型是否一致, 若一致, 则匹配成功
        
        self.mapping = {column : key}
        """
        mapping = dict()
        for column, datatype in self.dtype.items():
            for choice, confidence in process.extract(column, row.keys()):
                # 匹配度大于设定值
                if confidence >= criterion:
                    # row中的值的datatype在可接受的数据类型中
                    if type(row[choice]).__name__ in self.supported_dtype[datatype]:
                        mapping.setdefault(column, choice)
                        break
                    
        if len(self.dtype) != len(mapping): # 如果有column没有被匹配上
            raise Exception("Cannot find matching for {0}!".format(
                    set.difference(set(mapping.values()), set(self.dtype.values()))))
        else:
            self.mapping = mapping
    
    def set_mapping(self, mapping):
        """mapping
        """
        self.mapping = mapping
    
    def convert(self, row):
        new_row = dict()
        for column, key in self.mapping.items():
            new_row[column] = row[key]
        return new_row    
    
if __name__ == "__main__":
    from datetime import datetime, date
    sheet = Sheet({"name": "TEXT", "age": "INTEGER", "height": "REAL", 
                "date": "DATE", "datetime": "DATETIME"})
    
    # 自动检测匹配
    row = {"name": "Bill Gates", "age": 59, "height": 174.3, 
           "born_date": date(1959,10,28), "create_datetime": datetime.now()}
    sheet.match(row)
    print(sheet.convert(row))

    # 手动指定匹配
    row = {"firstname": "gates", "lastname": "bill", "fullname": "Bill Gates",
           "age": 59, "height": 174.3, 
           "born_date": date(1959,10,28), "create_datetime": datetime.now()}
    sheet.set_mapping({"name": "fullname", "age": "age", "height": "height",
                       "date": "born_date",  "datetime": "create_datetime"})
    print(sheet.convert(row))