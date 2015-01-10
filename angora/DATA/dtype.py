##encoding=utf8

"""
[EN] A set of tools for data structure and data type
[CN] 一些与基本数据结构，数据类型有关的工具箱
DtypeConverter 数据类型转换器
OrderedSet 有序集合
SuperSet 能同时intersect, union多个集合

import:
    from HSH.Data.dtype import OrderedSet
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
            next[1] = prev

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
            t = OrderedSet('simsalabim') # {"s", "i", "m", "a", "l", "b"}
            print(s | t) # s union t
            print(s & t) # s intersect t
            print(s - t) # s different t
        
        print("{:=^40}".format("test_OrderedSet"))
        orderedSet_UT1()
        orderedSet_UT2()
        
    test_OrderedSet()
    