##encoding=utf8

"""

pytimer
-------
    一个用于测量代码运行时间的小工具

compatibility: compatible to python2 and python3

prerequisites: None

import:
    from angora.GADGET.pytimmer import Timer
"""

from __future__ import print_function
import timeit
import time

class Timer():
    """Timer makes time measurement easy
    """
    def __init__(self):
        pass
    
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
    
    @staticmethod
    def test(func, howmany=1):
        """you can call this simply by Timer.test(func)
        """
        elapse = timeit.Timer(func).timeit(howmany)
        print("avg = %0.6f seconds, total = %0.6f seconds, times = %s" % (elapse/howmany, elapse, howmany) )
        
if __name__ == "__main__":
    timer = Timer()
    
    def usage_timer():
        array = list(range(1000000))
        
        timer.start()
        for index in range(len(array)):
            array[index]
        elapse = timer.timeup()
        print(elapse)
         
    usage_timer()
    
    def usage_timetest():
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
        
    usage_timetest()