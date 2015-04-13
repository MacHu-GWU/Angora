##encoding=utf-8

"""
Copyright (c) 2015 by Sanhe Hu
------------------------------
    Author: Sanhe Hu
    Email: husanhe@gmail.com
    Lisence: LGPL
    
    
Module description
------------------
    pytimer is a time application can easily measure, display and store time.
    
    
Keyword
-------
    time, gadget
    
    
Compatibility
-------------
    Python2: Yes
    Python3: Yes


Prerequisites
-------------
    None


Import Command
--------------
    from angora.GADGET.pytimmer import Timer
"""

from __future__ import print_function
import timeit
import time

class Timer():
    """Timer makes time measurement easy
    """
    def __init__(self):
        self.records = list()
        pass
    
    # === 单次计时器 ===
    def start(self):
        self.st = time.clock()
        
    def stop(self):
        """return the last measurement elapse time
        """
        self.elapse = time.clock() - self.st
        return self.elapse
        
    def display(self):
        """print the last measurement elapse time"""
        print("elapse %0.6f seconds" % self.elapse)

    def timeup(self):
        """print the last measurement elapse time, and return it"""
        elapse = self.stop()
        self.display()
        return elapse
    
    # === 函数性能计时器 ===
    @staticmethod
    def test(func, howmany=1):
        """you can call this simply by Timer.test(func)
        """
        elapse = timeit.Timer(func).timeit(howmany)
        print("avg = %0.6f seconds, total = %0.6f seconds, times = %s" % (elapse/howmany, elapse, howmany) )
    
    # === 多次计时器 ===
    def click(self):
        """record the current time and last time elapse. repeat this when you click() again
        """
        self.records.append(time.clock() - self.st)
        self.st = time.clock()

if __name__ == "__main__":
    timer = Timer()
    
    def usage_timer():
        """指定开始和结束
        """
        array = list(range(1000000))
        
        timer.start()
        for index in range(len(array)):
            array[index]
        elapse = timer.timeup()
        print(elapse)
         
#     usage_timer()
    
    def usage_timetest():
        """反复测量一段代码的速度, 取平均值
        """
        array = list(range(1000000))
        def iter_list1():
            for _ in array:
                pass
            
        def iter_list2():
            for index in range(len(array)):
                array[index]
        
        timer.test(iter_list1, 10)
        timer.test(iter_list2, 10)
        
        Timer.test(iter_list1, 10)
        Timer.test(iter_list2, 10)
        
#     usage_timetest()

    def usage_clicker():
        """在一个长程序中测量多个不同的时间点以及消耗的时间
        """
        timer.start()
        for i in range(1000000):
            if (i % 1000) == 0:
                timer.click()
        
        for i in timer.records:
            print(i)
        
#     usage_clicker()