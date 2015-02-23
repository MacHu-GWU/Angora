##encoding=utf8

"""
[EN] A set of tools for data structure and data type
[CN] 一些与基本数据结构，数据类型有关的工具箱
DtypeConverter 数据类型转换器
OrderedSet 有序集合
SuperSet 能同时intersect, union多个集合

import:
    from HSH.Data.dtype import OrderedSet, StrSet, IntSet, StrList, IntList
"""

from __future__ import print_function
import collections

class OrderedSet(collections.MutableSet):
    """Set that remembers original insertion order."""
    def __init__(self, iterable=None):
        self.end = end = [] 
        end += [None, end, end]         # sentinel node for doubly linked list
        self.map = {}                   # key --> [key, prev, next]
        if iterable is not None:
            self |= iterable

    def __len__(self):
        return len(self.map)

    def __contains__(self, key):
        return key in self.map

    def add(self, key):
        if key not in self.map:
            end = self.end
            curr = end[1]
            curr[2] = end[1] = self.map[key] = [key, curr, end]

    def discard(self, key):
        if key in self.map:        
            key, prev, next_item = self.map.pop(key)
            prev[2] = next_item
            next_item[1] = prev

    def __iter__(self):
        end = self.end
        curr = end[2]
        while curr is not end:
            yield curr[0]
            curr = curr[2]

    def __reversed__(self):
        end = self.end
        curr = end[1]
        while curr is not end:
            yield curr[0]
            curr = curr[1]

    def pop(self, last=True):
        if not self:
            raise KeyError('set is empty')
        key = self.end[1][0] if last else self.end[2][0]
        self.discard(key)
        return key

    def __repr__(self):
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, list(self))

    def __eq__(self, other):
        if isinstance(other, OrderedSet):
            return len(self) == len(other) and list(self) == list(other)
        return set(self) == set(other)

    @staticmethod
    def union(*argv):
        """顺序以第一个orderedset为准
        """
        res = OrderedSet()
        for ods in argv:
            res = res | ods
        return res
    
    @staticmethod
    def intersection(*argv):
        """顺序以第一个orderedset为准
        """
        res = OrderedSet(argv[0])
        for ods in argv:
            res = ods & res
        return res
    
class StrSet(set):
    """set that all elements are string"""
    @staticmethod
    def sqlite3_adaptor(_STRSET):
        """类 -> 字符串 转换"""
        return "&&".join(_STRSET)
    
    @staticmethod
    def sqlite3_converter(_STRING):
        """类 -> 字符串 转换"""
        try:
            return StrSet(_STRING.decode().split("&&"))
        except:
            return StrSet(_STRING.split("&&"))
        
class IntSet(set):
    """set that all elements are integer"""
    @staticmethod
    def sqlite3_adaptor(_INTSET):
        """类 -> 字符串 转换"""
        return "&&".join([str(i) for i in _INTSET])
    
    @staticmethod
    def sqlite3_converter(_STRING):
        """类 -> 字符串 转换"""
        try:
            return IntSet([int(s) for s in _STRING.decode().split("&&")])
        except:
            return IntSet([int(s) for s in _STRING.split("&&")])
        
class StrList(list):
    """list that all elements are string"""
    @staticmethod
    def sqlite3_adaptor(_STRLIST):
        """类 -> 字符串 转换"""
        return "&&".join(_STRLIST)
    
    @staticmethod
    def sqlite3_converter(_STRING):
        """类 -> 字符串 转换"""
        try:
            return StrList(_STRING.decode().split("&&"))
        except:
            return StrList(_STRING.split("&&"))
        
class IntList(list):
    """list that all elements are string"""
    @staticmethod
    def sqlite3_adaptor(_INTLIST):
        """类 -> 字符串 转换"""
        return "&&".join([str(i) for i in _INTLIST])
    
    @staticmethod
    def sqlite3_converter(_STRING):
        """类 -> 字符串 转换"""
        try:
            return IntList([int(s) for s in _STRING.decode().split("&&")])
        except:
            return IntList([int(s) for s in _STRING.split("&&")])
        
if __name__ == "__main__":    
    def test_OrderedSet():
        def orderedSet_UT1():
            print("{:=^30}".format("orderedSet_UT1"))
            s = OrderedSet(list())
            s.add("c")
            s.add("g")
            s.add("a")
            s.discard("g")
            print(s)
            print(list(s))
            
        def orderedSet_UT2():
            print("{:=^30}".format("orderedSet_UT2"))
            s = OrderedSet('abracadaba') # {"a", "b", "r", "c", "d"}
            t = OrderedSet('simcsalabim') # {"s", "i", "m", "c", "a", "l", "b"}
            print(s | t) # s union t
            print(s & t) # s intersect t
            print(s - t) # s different t

        def orderedSet_UT3():
            print("{:=^30}".format("orderedSet_UT3"))
            r = OrderedSet('buag') # {"b", "u", "a", "g"}
            s = OrderedSet('abracadaba') # {"a", "b", "r", "c", "d"}
            t = OrderedSet('simcsalabim') # {"s", "i", "m", "c", "a", "l", "b"}

            print(OrderedSet.union(r, s, t))
            print(OrderedSet.intersection(r, s, t))
            
        print("{:=^40}".format("test_OrderedSet"))
        orderedSet_UT1()
        orderedSet_UT2()
        orderedSet_UT3()
        
#     test_OrderedSet()

    def test_set_and_list():
        s = StrSet(["1", "2", "3"])
        s.add("4")
        print(s, type(s))
        
        l = StrList(["1", "2", "3"])
        l.append("4")
        print(l, type(l))
        
#     test_set_and_list()
    