##encoding=utf8

"""
compatibility: compatible to python2 and python3

prerequisites: None

import:
    from angora.GADGET.logicflow import tryit, timetest
"""

from __future__ import print_function

def tryit(howmany, func, *argv, **kwarg):
    """这个函数使用了一个重要的技巧将原函数的参数原封不动的封装成tryit这个函数的参数了
    用户只要跟使用func原函数一样使用tryit函数就好了，只不过在前面多了两个howmany和func的参数
        howmany 是尝试次数
        func 是你所要尝试的函数
        *argv, **kwarg 是func中的参数
        
    func函数一定要有如下特点：
        如果能正常运行，说明一定其中没有异常。
        如果有异常，一定要抛出异常，打断函数
    """
    flag = 1
    while flag <= howmany:
        try:
            return func(*argv, **kwarg)
        except Exception as e:
            current_exception = e
            flag += 1
    raise current_exception

if __name__ == "__main__":
    def usage_tryit():
        import random
        def gamble(bet_on, number_of_choice):
            if bet_on == random.randint(1, number_of_choice):
                print("it's %s, you win !!" % bet_on)
            else:
                raise Exception("Sorry, you lose!!")
        
        try: # 尝试五次游戏， 压住在3上, 结果可能是1-10之间的一个
            tryit(5, gamble, 3, 10)
        except Exception as e:
            print(e)
    
    usage_tryit()