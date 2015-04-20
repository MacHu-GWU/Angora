##encoding=utf-8

"""
Copyright (c) 2015 by Sanhe Hu
------------------------------
    Author: Sanhe Hu
    Email: husanhe@gmail.com
    Lisence: LGPL
    

Module description
------------------
    This module is provide methods for searching item in a sorted list.
    find_last_true() is a magic method, please see function doc str for more info.
    
    
Keyword
-------
    algorithm, bineary search
    
    
Compatibility
-------------
    Python2: Yes
    Python3: Yes
    
    
Prerequisites
-------------
    None


Import Command
--------------
    from angora.DATA.binarysearch import (find_index, find_lt, find_le, find_gt, find_ge,
        find_last_true, find_nearest)
"""

from __future__ import print_function
import bisect

########################################################################
# official recepi from doc.python.org                                  #
# https://docs.python.org/2/library/bisect.html#searching-sorted-lists #
########################################################################

def find_index(array, x):
    "Locate the leftmost value exactly equal to x"
    i = bisect.bisect_left(array, x)
    if i != len(array) and array[i] == x:
        return i
    raise ValueError

def find_lt(array, x):
    "Find rightmost value less than x"
    i = bisect.bisect_left(array, x)
    if i:
        return array[i-1]
    raise ValueError

def find_le(array, x):
    "Find rightmost value less than or equal to x"
    i = bisect.bisect_right(array, x)
    if i:
        return array[i-1]
    raise ValueError

def find_gt(array, x):
    "Find leftmost value greater than x"
    i = bisect.bisect_right(array, x)
    if i != len(array):
        return array[i]
    raise ValueError

def find_ge(array, x):
    "Find leftmost item greater than or equal to x"
    i = bisect.bisect_left(array, x)
    if i != len(array):
        return array[i]
    raise ValueError

def find_last_true(sorted_list, true_criterion):
    """
    [EN doc]
    suppose we have a list of item [item1, item2, ..., itemn]
    if we do a map:
        list of items -- tru_criterion --> [True, True, ... True, False, False, ... False]
                                                           (last true)
    this function returns the index of last true item.
    
    we do can do the map for all item, and run a binary search to find the index. But sometime
    the mapping function is expensive. so this function gives a way to minimize the time cost.
    
    [CN doc]
    假设有一组排序号了的元素, 从前往后假设前面的元素都满足某一条件, 而到了中间某处起就不再满足了。
    本函数返回满足这一条件的最后一个元素。
    
    例题:
        序号   [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        真值表 [1, 1, 1, 1, 1, 1, 1, 0, 0, 0]
        我们要找到那个小于等于6的元素
    
    算法:
        我们检验一个序号ind, 如果为False, 那么则向前跳跃继续检验
        如果为True, 那么则检验ind+1, 如果为False, 说明找到了。如果ind+1也为真, 则向后跳跃。重复这一过程。
    
    例:
        第一次检查 int((0+9)/2.0) = 4, 为True,
        检查4+1=5, 也是True。 那么跳跃至 int((4+9)/2.0)=6。很显然, 我们找到了
    """
    
    # exam first item, if not true, then impossible to find result
    if not true_criterion(sorted_list[0]):
        raise ValueError
    
    # exam last item, if true, it is the one.
    if true_criterion(sorted_list[-1]):
        return sorted_list[-1]
    
    lower, upper = 0, len(sorted_list) - 1

    index = int((lower+upper)/2.0)
    while 1:
        if true_criterion(sorted_list[index]):
            if true_criterion(sorted_list[index+1]):
                lower = index
                index = int((index+upper)/2.0)
            else:
                return sorted_list[index]
        else:
            upper = index
            index = int((lower+index)/2.0)
            
def find_nearest(array, x):
    """find the nearest item of x from sorted array
    """
    if x <= array[0]:
        return array[0]
    elif x >= array[-1]:
        return array[-1]
    else:
        lower = find_le(array, x)
        upper = find_ge(array, x)
        if (x - lower) > (upper - x):
            return upper
        else:
            return lower

if __name__ == "__main__":
    from collections import OrderedDict
    import unittest
    import random
    import time

    class BiSearch():
        """A binary search class, doens't have better performance than original implementation
        """
        def fit(self, array):
            self.train_dict = OrderedDict()
            for ind, value in enumerate(array):
                self.train_dict[ind] = value
            self.train_array = array
        
        def find_le(self, x):
            "Find rightmost value less than or equal to x"
            i = bisect.bisect_right(self.train_array, x)
            if i != len(self.train_array):
                return self.train_dict[i-1]
            raise ValueError
    
        def find_ge(self, x):
            "Find leftmost item greater than or equal to x"
            i = bisect.bisect_left(self.train_array, x)
            if i != len(self.train_array):
                return self.train_dict[i]
            raise ValueError
    
    class FunctionsUnittest(unittest.TestCase):
        def setUp(self):
            self.sorted_array = list(range(1000))
         
        def test_index(self):
            self.assertEqual(find_index(self.sorted_array, 0), 0)
            self.assertEqual(find_index(self.sorted_array, 999), 999)
            self.assertEqual(find_index(self.sorted_array, 499), 499)
            self.assertRaises(ValueError, find_index, self.sorted_array, -1)
            self.assertRaises(ValueError, find_index, self.sorted_array, 1001)
     
        def test_find_nearest(self):
            self.assertEqual(find_nearest(self.sorted_array, 25), 25)
            self.assertEqual(find_nearest(self.sorted_array, 25.49), 25)
            self.assertEqual(find_nearest(self.sorted_array, 25.5), 25)
            self.assertEqual(find_nearest(self.sorted_array, 25.51), 26)
            
            self.assertEqual(find_nearest(self.sorted_array, -1), 0)
            self.assertEqual(find_nearest(self.sorted_array, 1000), 999)
            
    class PerformanceTest(unittest.TestCase):
        def setUp(self):
            self.sorted_array = list(range(1000*1000))
            self.bisearch = BiSearch()
            self.bisearch.fit(self.sorted_array)
              
        def test_speed(self):
            """because original recepi use list[index] to take item. I thought the speed can be
            improved if I use dict[index]. But failed.
            """
            st = time.clock()
            for _ in range(1000):
                find_le(self.sorted_array, 500*1000)
            original = time.clock() - st
  
            st = time.clock()
            for _ in range(1000):
                self.bisearch.find_le(500*1000)
            improved = time.clock() - st
            self.assertFalse(improved < original) # improved elapse not smaller than original
     
    class LastTrueTest(unittest.TestCase):
        def setUp(self):
            self.sorted_list = list({random.randint(1, 100000) for _ in range(1000)})
            self.sorted_list.sort()
             
        def true_criterion(self, item):
            return item <= 500
         
        def test(self):
            value = find_last_true(self.sorted_list, self.true_criterion)
            print("last True value is %s" % value)
            
    unittest.main()
