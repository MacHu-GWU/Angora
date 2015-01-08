##encoding=utf8

from __future__ import print_function
try: # 可以一次性全部倒入
    from angora.DATASCI import *
except: # 也可以单独导入
    from angora.DATASCI import knn
    from angora.DATASCI import linreg
    from angora.DATASCI import preprocess
    from angora.DATASCI import psmatcher
    from angora.DATASCI import stat
    
def test_import():
    print(knn.dist)
    print(knn.knn_classify)
    print(knn.knn_find)
    print(linreg.linreg_predict)
    print(linreg.linreg_coef)
    print(linreg.glance_2d)
    
test_import()