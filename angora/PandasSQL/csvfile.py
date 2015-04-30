##encoding=utf-8

"""

为了轻松的将CSV中的数据Map到数据库中, 我们将整个步骤分为两步:

1. 读取CSV文件到pandas.DataFrame, 确保里面的格式正确
2. Map pandas.DataFrame.columns 中的index到数据库中的列名

第一步. 读取CSV文件
-------------------
read_header = True


"""

from angora.DATA.timewrapper import timewrapper
import numpy as np, pandas as pd

class CSVFile():
    dtype_mapping = {"TEXT": np.str, "INTEGER": np.int64, "REAL": np.float64,
                     "DATE": np.str, "DATETIME": np.str}
    
    def __init__(self, abspath, sep = ",", header = True, 
                 usecols = None, dtype = None, converter = None):
        """
        [args]
        ------
            abspath: 
                csv file absolute path
                
            sep: 
                csv seperator, default ','
                
            header: 
                has header?
                
            usecols:
                a index list tells which columns you want to use. for example [1, 4, 5] means
                you only use the second, fifth and sixth columns
                
            dtype: 
                define the data type for each columns in a dictionary. valid dtypes are: 
                    TEXT, INTEGER, REAL, DATE, DATETIME
                example1: {"column_name1": "TEXT", "column_name2": "INTEGER"}
                example2: {column_index_number1: "TEXT", column_index_number2: "INTEGER"}
        """
        self.abspath = abspath
        self.sep = sep
        if header:
            self.header = 0
        else:
            self.header = None
        self.usecols = usecols
        
        # 检查header, 如果没有, 则自动将header设置为 prefix+column_number
        # 并且将dtype中的column序号也转变成prefix+column_number
        if header: 
            self.prefix = None
            self.dtype = dtype
        else:
            self.prefix = "column"
            fixed_dtype = dict()
            for k, v in dtype.items():
                fixed_dtype[self.prefix + str(k)] = v
            self.dtype = fixed_dtype
        
        if dtype: # 将用户定义的dtype转化成pandas.read_csv可用的数据类型
            try: # 检查dtype中数据类型是否定义正确
                self.pd_dtype = {k: self.dtype_mapping[v] for k, v in dtype.items()}
            except:
                raise Exception("data type has to be one of ['TEXT', 'INTEGER', 'REAL', 'DATE', 'DATETIME']")
        else:
            self.pd_dtype = None
        
        self.converter = self.do_nothing
        
#         try: # 读取第一条数据作为样本储存起来
        self.read_sample()
#         except:
#             pass
    
    def do_nothing(self, d):
        """对传入的dict什么都不做
        """
        return d
    
    def plugin_converter(self, converter):
        """将self.converter方法绑定到converter函数上, 对传入的dict进行一定的修改
        此方法可插拔
        """
        self.converter = converter
    
    def read_sample(self):
        """读取第一条数据作为样本储存起来
        """
        df = pd.read_csv(self.abspath, sep=self.sep, header=self.header, 
                         nrows=1, dtype=self.pd_dtype, usecols=self.usecols,
                         prefix=self.prefix)
        row = df.iloc[0,:].to_dict()
        for column, datatype in self.dtype.items():
            if datatype == "DATE":
                row[column] = timewrapper.str2date(row[column])
            if datatype == "DATETIME":
                row[column] = timewrapper.str2datetime(row[column])
        self.sample = self.converter(row)
        
    def generate_rows(self, chunksize=1024):
        """从csv文件中生成row数据
        """
        for df in pd.read_csv(self.abspath, 
                              sep=self.sep, 
                              header=self.header,
                              dtype=self.pd_dtype,
                              usecols=self.usecols, 
                              prefix=self.prefix,
                              iterator=True, 
                              chunksize=chunksize):
            for column, datatype in self.dtype.items():
                if datatype == "DATE":
                    series = list()
                    for i in df[column]:
                        try:
                            series.append(timewrapper.str2date(i))
                        except:
                            series.append(None)
                    df[column] = series
                if datatype == "DATETIME":
                    series = list()
                    for i in df[column]:
                        try:
                            series.append(timewrapper.str2datetime(i))
                        except:
                            series.append(None)
                    df[column] = series
                    
            for _, series in df.iterrows():
                yield self.converter( series.to_dict() )
        
if __name__ == "__main__":
    from angora.STRING.formatmaster import fmter
    from sheet import Sheet
    from datetime import datetime, date
    
    def create_testdata():
        df = [["eid001", 100, 1.23, date.today(), datetime.now()]]
        df = pd.DataFrame(df, columns=["TEXT", "INTEGER", "REAL", "DATE", "DATETIME"])
        df.to_csv(r"testdata\with_header.txt", index=False)
        df.to_csv(r"testdata\without_header.txt", header=False, index=False)
    
#     create_testdata()

    # 定义你目标数据格式
    sheet = Sheet({"_id": "TEXT", "age": "INTEGER", "height": "REAL", 
                   "create_date": "DATE", "datetime": "DATETIME"})

    csvfile_with_header = CSVFile(r"testdata\with_header.txt",
                      sep=",",
                      header=True,
                      usecols=[0,1,2,3,4],
                      dtype={"DATE": "DATE", "DATETIME": "DATETIME"})
    sheet.match(csvfile_with_header.sample)
    for row in csvfile_with_header.generate_rows():
        print( sheet.convert(row) )
        
        
    csvfile_without_header = CSVFile(r"testdata\without_header.txt",
                      sep=",",
                      header=False,
                      usecols=[0,1,2,3,4],
                      dtype={3: "DATE", 4: "DATETIME"})
    sheet.match(csvfile_without_header.sample)
    for row in csvfile_without_header.generate_rows():
        print( sheet.convert(row) )

    print("Complete")