##encoding=utf-8

"""
Copyright (c) 2015 by Sanhe Hu
------------------------------
    Author: Sanhe Hu
    Email: husanhe@gmail.com
    Lisence: LGPL
    
Module description
------------------
    [EN] A set of tools for data structure and data type
    [CN] 一些与基本数据结构，数据类型有关的工具箱
        OrderedSet 有序集合
        StrSet, IntSet, StrList, IntList, 用于将 字符串/整数 集合/数组 转化为字符串。
            以用于数据库的IO

Keyword
-------
    date type, set, list
    

Compatibility
-------------
    Python2: Yes
    Python3: Yes


Prerequisites
-------------
    None


Import Command
--------------
    from angora.Data.dtype import OrderedSet, StrSet, IntSet, StrList, IntList
"""

from __future__ import print_function
import collections

class OrderedSet(collections.MutableSet):
    """Set that remembers original insertion order.
    orginally from http://code.activestate.com/recipes/576694/
    add union(*argv), intersect(*argv) method
    """
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
            raise KeyError("set is empty")
        key = self.end[1][0] if last else self.end[2][0]
        self.discard(key)
        return key

    def __repr__(self):
        if not self:
            return "%s()" % (self.__class__.__name__,)
        return "%s(%r)" % (self.__class__.__name__, list(self))

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
    import unittest
    
    class UserDefinedUnittest(unittest.TestCase):
        def test_OrderedSet1(self):
            s = OrderedSet(list())
            s.add("c")
            s.add("g")
            s.add("a")
            self.assertListEqual(list(s), ["c", "g", "a"])
            s.discard("g")
            self.assertListEqual(list(s), ["c", "a"])
        
        def test_OrderedSet2(self):
            s = OrderedSet("abracadaba") # {"a", "b", "r", "c", "d"}
            t = OrderedSet("simcsalabim") # {"s", "i", "m", "c", "a", "l", "b"}
            self.assertListEqual(list(s | t), ["a", "b", "r", "c", "d", "s", "i", "m", "l"]) # s union t
            self.assertListEqual(list(s & t), ["c", "a", "b"]) # s intersect t
            self.assertListEqual(list(s - t), ["r", "d"]) # s different t
        
        def test_OrderedSet3(self):
            r = OrderedSet("buag") # {"b", "u", "a", "g"}
            s = OrderedSet("abracadaba") # {"a", "b", "r", "c", "d"}
            t = OrderedSet("simcsalabim") # {"s", "i", "m", "c", "a", "l", "b"}
            self.assertListEqual(list(OrderedSet.union(r, s, t)), # r union s union t
                                 ["b", "u", "a", "g", "r", "c", "d", "s", "i", "m", "l"]) 
            self.assertListEqual(list(OrderedSet.intersection(r, s, t)), # r intsect s and t
                                 ["b", "a"])
        
        def test_all(self): # use chinese character to text utf-8 compatibility
            strset = StrSet(["是", "是", "否", "否"])
            self.assertIn(StrSet.sqlite3_adaptor(strset), ["是&&否", "否&&是"])
            self.assertSetEqual(StrSet.sqlite3_converter("是&&否"), StrSet(["是", "否"]))
            
            intset = IntSet([1, 1, 2, 2])
            self.assertIn(IntSet.sqlite3_adaptor(intset), ["1&&2", "2&&1"])
            self.assertSetEqual(IntSet.sqlite3_converter("1&&2"), StrSet([1, 2]))
            
            strlist = StrList(["是", "是", "否", "否"])
            self.assertIn(StrList.sqlite3_adaptor(strlist), ["是&&是&&否&&否"])
            self.assertListEqual(StrList.sqlite3_converter("是&&否"), StrList(["是", "否"]))
            
            intlist = IntList([1, 1, 2, 2])
            self.assertIn(IntList.sqlite3_adaptor(intlist), ["1&&1&&2&&2"])
            self.assertListEqual(IntList.sqlite3_converter("1&&2"), IntList([1, 2]))           
            
    unittest.main()