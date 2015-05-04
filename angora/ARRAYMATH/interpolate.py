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
    None

Import Command
--------------
    from angora.DATASCI.interpolate import LinearInterpolator, arange
"""

import bisect
import sys
try: # if angora not installed, then interpolate datetime are not working
    from angora.DATA.timewrapper import timewrapper
except:
    pass

is_py2 = (sys.version_info[0] == 2)
if is_py2:
    range = xrange
    
def find_lt(array, x):
    "Find rightmost item index less than x"
    i = bisect.bisect_left(array, x)
    return i-1

def find_le(array, x):
    "Find rightmost item index less than or equal to x"
    i = bisect.bisect_right(array, x)
    return i-1

def find_gt(array, x):
    "Find leftmost item index greater than x"
    i = bisect.bisect_right(array, x)
    return i

def find_ge(array, x):
    "Find leftmost item index greater than or equal to x"
    i = bisect.bisect_left(array, x)
    return i

class LinearInterpolator():
    def __init__(self, x_old, y_old):
        if (len(x_old) == len(y_old) and len(x_old) >= 2):
            self.x_old = x_old
            self.y_old = y_old
            self.first = x_old[0]
            self.last = x_old[-1]
        else:
            raise Exception("Length of oldX doesn't match oldY, or the size not larger than 1.")


    def locate(self, x1, y1, x2, y2, x3):
        """given 2 points (x1, y1), (x2, y2), find y3 for x3.
        """
        return y1 - 1.0 * (y1 - y2) * (x1 - x3) / (x1 - x2)

    def simple_case_interpolate(self, x_old, y_old, x_new):
        """simple case means, x_new[0] >= x_old[0] and x_new[-1] <= x_old[-1]
        it's an O(n) algorithm
        """
        index = find_le(x_old, x_new[0]) # trim it can boost the speed
        x_old = x_old[index:][::-1] 
        y_old = y_old[index:][::-1]
        y_new = list()

        for i in x_new:
            while 1:
                try:
                    xx, yy = x_old.pop(), y_old.pop()
                    if yy <= i:
                        left_x = xx
                        left_y = yy
                    else:
                        right_x = xx
                        right_y = yy
                        x_old.append(right_x)
                        x_old.append(left_x)
                        y_old.append(right_y)
                        y_old.append(left_y)
                        y_new.append(self.locate(left_x, left_y, right_x, right_y, i))
                        break
                except:
                    y_new.append(left_y)
                    break
        return y_new

    def simple_case_interpolate_slower_way(self, x_old, y_old, x_new):
        """simple case means, x_new[0] >= x_old[0] and x_new[-1] <= x_old[-1]
        it's an O(nlog(n)) algorithm
        """
        x_old, y_old = x_old[::-1], y_old[::-1]
        y_new = list()

        for i in x_new:
            while 1:
                try:
                    xx, yy = x_old.pop(), y_old.pop()
                    if yy <= i:
                        left_x = xx
                        left_y = yy
                    else:
                        right_x = xx
                        right_y = yy
                        x_old.append(right_x)
                        x_old.append(left_x)
                        y_old.append(right_y)
                        y_old.append(left_y)
                        y_new.append(self.locate(left_x, left_y, right_x, right_y, i))
                        break
                except:
                    y_new.append(left_y)
                    break
        return y_new
    
    def bineary_search_interpolate(self, x_old, y_old, x_new):
        """Interpolate value to new x axis. O(n*log(n)) implementation. Using binary
        search to find left greatest equal and right smallest value. This is a legacy
        method and don't use.
        """
        y_new = list()
        for i in x_new:
            try:
                ind1 = find_le(x_old, i)
                ind2 = find_gt(x_old, i)
                x1, y1, x2, y2 = x_old[ind1], y_old[ind1], x_old[ind2], y_old[ind2]
                y_new.append(self.locate(x1, y1, x2, y2, i))
            except:
                ind1 = find_lt(x_old, i)
                ind2 = find_ge(x_old, i)
                x1, y1, x2, y2 = x_old[ind1], x_old[ind1], x_old[ind2], x_old[ind2]
                y_new.append(self.locate(x1, y1, x2, y2, i))        
        return y_new

    def interpolate(self, x_new):
        """the universal robust interpolate method
        """
        if x_new[0] < self.first:
            left_pad_x = [x_new[0],]
            left_pad_y = [self.locate(self.x_old[0], self.y_old[0], 
                                   self.x_old[1], self.y_old[1], x_new[0]),]
        else:
            left_pad_x, left_pad_y = [], []
        if x_new[-1] > self.last:
            right_pad_x = [x_new[-1],]
            right_pad_y = [self.locate(self.x_old[-2], self.y_old[-2], 
                                    self.x_old[-1], self.y_old[-1], x_new[-1]),]
        else:
            right_pad_x, right_pad_y = [], []
        x_old = left_pad_x + self.x_old + right_pad_x
        y_old = left_pad_y + self.y_old + right_pad_y
        return self.simple_case_interpolate(x_old, y_old, x_new)

        
    def __call__(self, x_new):
        return self.interpolate(x_new)

   
def linear_interp(x_old, y_old, x_new):
    f = LinearInterpolator(x_old, y_old)
    return f(x_new)

def linear_interp_datetime(x_old, y_old, x_new):
    x_old = [timewrapper.totimestamp(x) for x in x_old]
    x_new = [timewrapper.totimestamp(x) for x in x_new]
    f = LinearInterpolator(x_old, y_old)
    return f(x_new)

def check_reliability(x_old, x_new, bound):
    """Because we got x_new interpolated from x_old, so we have to label new
    time point as reliable or not reliable. The rule is this:
        if distance of new time point to the nearest old time point <= 600 seconds:
            we can trust it
        else:
            we cannot trust it
            
    Here is an O(n) algorithm solution. A lots of improvement than old O(n^2) one.
    """
    # add pad that is absolutely out of boundary to the left and right
    # so the algorithm can still handle out-of-boundary case.
    if x_new[0] < x_old[0]:
        left_pad_x = [x_new[0] - 2 * bound,]
    else:
        left_pad_x = []
    if x_new[-1] > x_old[-1]:
        right_pad_x = [x_new[-1] + 2* bound,]
    else:
        right_pad_x = []
    x_old = left_pad_x + x_old + right_pad_x
    
    x_old = x_old[::-1]
    reliable_flag = list()
    for t in x_new:
        while 1:
            try:
                x = x_old.pop()
                if x <= t: # if it's a left bound, then continue for right bound
                    left = x
                else: # if it's a right bound, calculate reliability
                    right = x
                    x_old.append(right)
                    x_old.append(left)
                    left_dist, right_dist = t - left, right - t
                    if left_dist <= right_dist:
                        reliable_flag.append(left_dist)
                    else:
                        reliable_flag.append(right_dist)
                    break
            except: # it there's no value to pop out from x_old, the use 
                reliable_flag.append(t - left)
                break
            
    int_reliable_flag = list()
    for i in reliable_flag:
        if i <= bound:
            int_reliable_flag.append(1)
        else:
            int_reliable_flag.append(0)
    return int_reliable_flag
    
def check_reliability_datetime(self, x_new, seconds):
    x_old = [timewrapper.totimestamp(x) for x in self.x_old]
    x_new = [timewrapper.totimestamp(x) for x in x_new]
    return check_reliability(x_old, x_new, seconds)

def arange(start=None, end=None, count=None, gap=None):
    """
    start, end, count
    start, end, freq
    start, count, freq
    end, count, freq
    """
    if (bool(start) + bool(end) + bool(count) + bool(gap)) != 3:
        raise Exception("Must specify three of start, end, count or gap")
    
    array = list()
    if not start:
        start = end - count * gap
        for _ in range(count):
            start += gap
            array.append(start)
        return array
    
    elif not end:
        start -= gap
        for _ in range(count):
            start += gap
            array.append(start)
        return array
    
    elif not count:
        start -= gap
        for _ in range(2**28):
            start += gap
            if start <= end:
                array.append(start)
            else:
                return array
                
    else:
        gap = 1.0 * (end - start) / (count - 1)
        start -= gap
        for _ in range(count):
            start += gap
            array.append(start)
        return array


if __name__ == "__main__":
    import unittest
    import time
    
    class LinearInterpolatorUnittest(unittest.TestCase):
        def test_simple_case_interpolation(self):
            x_old, y_old = [1, 2, 3], [1, 2, 3]
            x_new1 = [1, 1.5, 2, 2.5, 3]
            x_new2 = [1.5, 2, 2.5]
            f = LinearInterpolator(x_old, y_old)
            self.assertEqual(f.simple_case_interpolate(x_old, y_old, x_new1),
                             x_new1)
            self.assertEqual(f.simple_case_interpolate(x_old, y_old, x_new2),
                             x_new2) 

            self.assertEqual(f.simple_case_interpolate_slower_way(x_old, y_old, x_new1),
                             x_new1)
            self.assertEqual(f.simple_case_interpolate_slower_way(x_old, y_old, x_new2),
                             x_new2) 
                 
        def test_bineary_search_interpolate(self):
            x_old, y_old = [1, 2, 3], [1, 2, 3]
            x_new1 = [1, 1.5, 2, 2.5, 3]
            x_new2 = [1.5, 2, 2.5]
            f = LinearInterpolator(x_old, y_old)
            self.assertEqual(f.bineary_search_interpolate(x_old, y_old, x_new1),
                             x_new1)
            self.assertEqual(f.bineary_search_interpolate(x_old, y_old, x_new2),
                             x_new2) 
        
        
        def test_interpolate(self):
            x_old, y_old = [1, 2, 3], [1, 2, 3]
            x_new = [0, 1.5, 3, 4.5]
            f = LinearInterpolator(x_old, y_old)
            self.assertEqual(f.interpolate(x_new), x_new)
             
        def test_performance(self): # 0.155 ~ 0.165
            x_old = arange(start=1, end=1000000, gap=1)
            y_old = arange(start=1, end=1000000, gap=1)
            x_new1 = arange(start=300000, end=700000, count=654321)
            x_new2 = arange(start=-100, end=1000100, count=654321)
            f = LinearInterpolator(x_old, y_old)
              
            st = time.clock()
            y_new1 = f.simple_case_interpolate(x_old, y_old, x_new1)
            print(time.clock()-st)
  
            st = time.clock()
            y_new2 = f.simple_case_interpolate_slower_way(x_old, y_old, x_new1)
            print(time.clock()-st)
               
            st = time.clock()
            y_new3 = f.bineary_search_interpolate(x_old, y_old, x_new1)
            print(time.clock()-st)
             
            st = time.clock()
            y_new4 = f.interpolate(x_new2)
            print(time.clock()-st)
             
            self.assertListEqual(y_new1, y_new2)
            self.assertListEqual(y_new2, y_new3)
        
        def test_check_reliability(self):
            x_old = [0, 5, 100]
            x_new = [-1, 3, 5, 9, 50]
            int_reliability_flag = check_reliability(x_old, x_new, 3.5)
            self.assertListEqual(int_reliability_flag, [1, 1, 1, 0, 0])
            
    class arangeUnittest():
        def test_functionality(self):
            self.assertListEqual(
                arange(end=10, count=10, gap=1),
                [1,2,3,4,5,6,7,8,9,10],
                )
            self.assertListEqual(
                arange(start=1, count=10, gap=1),
                [1,2,3,4,5,6,7,8,9,10],
                )
            self.assertListEqual(
                arange(start=1, end=10, gap=1),
                [1,2,3,4,5,6,7,8,9,10],
                )
            self.assertListEqual(
                arange(start=1, end=10, count=10),
                [1,2,3,4,5,6,7,8,9,10],
                )
            
    unittest.main()
