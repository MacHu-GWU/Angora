##encoding=utf8

from __future__ import print_function
import time

st = time.clock()
from angora.DATA import *
print(time.clock() - st)

# st = time.clock()
# from angora.DBA.SQLITE.core import *
# print(time.clock() - st)

st = time.clock()
from angora.SQLITE import *
print(time.clock() - st)

print(Table)