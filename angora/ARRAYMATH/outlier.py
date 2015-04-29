##encoding=utf-8

"""
Copyright (c) 2015 by Sanhe Hu
------------------------------
    Author: Sanhe Hu
    Email: husanhe@gmail.com
    Lisence: LGPL
    
Compatibility
-------------
    IronPython2.7: Yes

Prerequisites
-------------
    statistics: A Python 2.* port of 3.4 Statistics Module

Import Command
--------------
    from angora.DATASCI.outlier import find_outlier, clear_outlier_onetime, clear_outlier_literally
"""

from __future__ import print_function
from statistics import mean, stdev

def find_outlier(array, outlier_criterion = 2):
    """return list of outliers
    [Args]
    ------
        array: list of numbers
     
        outlier_criterion: sample with n of standard deviation bias 
            from mean value will be considered as outlier
     
    [Returns]
    ---------
        outliers: list of outliers, maintain the order in original array
    """
    m, std = mean(array), stdev(array)
    outliers = list()
    for i in array:
        if abs(i - m) > outlier_criterion * std:
            outliers.append(i)
    return outliers

def clear_outlier_onetime(array, outlier_criterion = 2):
    """remove outliers by criterion then returns.
    [Args]
    ------
        array: list of numbers
     
        outlier_criterion: sample with n of standard deviation bias 
            from mean value will be considered as outlier
     
    [Returns]
    ---------
        clean_array: array with all outliers deleted
    """
    m, std = mean(array), stdev(array)
    clean_array = list()
    for i in array:
        if abs(i - m) <= outlier_criterion * std:
            clean_array.append(i)
    return clean_array
    
def clear_outlier_literally(array, outlier_criterion = 2):
    """recurrsively remove outliers, until there's no outliers at all. Then return.
    [Args]
    ------
        array: list of numbers
     
        outlier_criterion: sample with n of standard deviation bias 
            from mean value will be considered as outlier
     
    [Returns]
    ---------
        array: array with all outliers deleted, and there's no more outliers can be found
    """
    while 1:
        length = len(array)
        array = clear_outlier_onetime(array, outlier_criterion)
        if length == len(array):
            return array
  
if __name__ == "__main__":
    import unittest
    
    class OutlierUnittest(unittest.TestCase):
        def setUp(self):
            self.array = [1,2,3,4,5,6,7,8,9]
        
        def test_find_outlier(self):
            self.assertListEqual(find_outlier(self.array, 1), [1,2,8,9])

        def test_clear_outlier_onetime(self):
            self.assertListEqual(clear_outlier_onetime(self.array, 1), [3,4,5,6,7])
            
        def test_clear_outlier_literally(self):
            self.assertListEqual(clear_outlier_literally(self.array, 1), [4,5,6])
            
    unittest.main()