##encoding=utf-8



try:
    from fuzzywuzzy import process
except:
    pass
from angora.DATA.timewrapper import timewrapper
from datetime import datetime, date
import numpy as np

class DtypeConverter():
    def __init__(self):
        self.rules = {
            "TEXT": self.to_int,
            "REAL": self.to_float,
            }
        
    def _int2int(self, value):
        return value
    
    def _float2int(self, value):
        return int(value)
    
    def _str2int(self, value):
        try:
            return int(value)
        except:
            res = list()
            for char in value:
                if char.isdigit():
                    res.append(char)
            return int("".join(res))
    
    def _date2int(self, value):
        return value.toordinal()
    
    def _datetime2int(self, value):
        return int(timewrapper.totimestamp(value))
    
    def batch_to_int(self, value):
        """
        """
        try:
            return self.default_2int(value)
        except:
            if type(value).__name__ in ["int", "int8", "int16", "int32", "int64"]:
                self.default_2int = self._int2int
                return self._int2int(value)
            elif type(value).__name__ in ["float", "float16", "float32", "float64"]:
                self.default_2int = self._float2int
                return self._float2int(value)
            elif isinstance(value, str):
                self.default_2int = self._str2int
                return self._str2int(value)
            elif isinstance(value, date):
                self.default_2int = self._date2int
                return self._date2int(value)
            elif isinstance(value, datetime):
                self.default_2int = self._datetime2int
                return self._datetime2int(value)
            else:
#                 raise Exception("unable to convert ==> %s <== to int" % repr(value))
                return None
                
        
    def _int2float(self, value):
        return float(value)
    
    def _float2float(self, value):
        return float(value)
    
    def _str2float(self, value):
        try:
            return float(value)
        except:
            try:
                res = list()
                for char in value:
                    if (char.isdigit() or char == "."):
                        res.append(char)
                return float("".join(char))
            except:
                return None
    
    def _datetime2float(self, value):
        return timewrapper.totimestamp(value)

    def to_float(self, value):
        try:
            return self.default_2float(value)
        except:
            if type(value).__name__ in ["int", "int8", "int16", "int32", "int64"]:
                self.default_2float = self._int2float
                return self._int2float(value)
            elif type(value).__name__ in ["float", "float16", "float32", "float64"]:
                self.default_2float = self._float2float
                return self._float2float(value)
            elif isinstance(value, str):
                self.default_2float = self._str2float
                return self._str2float(value)
            elif isinstance(value, datetime):
                self.default_2float = self._datetime2float
                return self._datetime2float(value)
            else:
                raise Exception("unable to convert ==> %s <== to int" % repr(value))

dtcvt = DtypeConverter()

if __name__ == "__main__":
    import unittest
    
    class DtypeConverterUnittest(unittest.TestCase):
        def test_all(self):
            self.assertEqual(dtcvt.to_int(1), 1)
            self.assertEqual(dtcvt.to_int(1.5), 1)
            self.assertEqual(dtcvt.to_int("123"), 123)
            self.assertEqual(dtcvt.to_int("abc123xyz"), 123)
            self.assertEqual(dtcvt.to_int(date(2000,1,1)), 730120)
            self.assertEqual(dtcvt.to_int(datetime(2000,1,1,0,0,0)), 1)
            self.assertEqual(dtcvt.to_int([1,2,3]), 1)
            
    unittest.main()
    print([dtcvt.to_int(date(2000,1,1))])