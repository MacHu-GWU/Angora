##encoding=UTF-8

from __future__ import print_function

def binary_search(sorted_list, true_criterion):
    """假设有一组排序号了的元素, 从前往后假设前面的元素都满足某一条件, 而到了中间某处起就不再满足了。
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
    if true_criterion(sorted_list[-1]): # exam last item, if true, it is the one.
        return sorted_list[-1]
    
    data = {ind: item for ind, item in enumerate(sorted_list)} # create a hash map
    indice = list(data.keys())
    lower, upper = indice[0], indice[-1]

    index = int((lower+upper)/2.0)
    while 1:
        if true_criterion(data[index]):
            if true_criterion(data[index+1]):
                lower = index
                index = int((index+upper)/2.0)
            else:
                return data[index]
        else:
            upper = index
            index = int((lower+index)/2.0)
    
if __name__ == "__main__":
    def binary_search_performance_test():
        import random
        import time
        # 找到一个列表中最大的那个小于500000的数
        sorted_list = list({random.randint(1, 1000000) for i in range(1000000)})
        sorted_list.sort()
        def true_criterion(item):
            return item <= 500000
        
        data = {ind: item for ind, item in enumerate(sorted_list)} # create a hash map
        indice = list(data.keys())
        
        st = time.clock()
        lower, upper = indice[0], indice[-1]
        index = int((lower+upper)/2.0)
        while 1:
            if true_criterion(data[index]):
                if true_criterion(data[index+1]):
                    lower = index
                    index = int((index+upper)/2.0)
                else:
                    break
            else:
                upper = index
                index = int((lower+index)/2.0)
        print(data[index])
        print(time.clock() - st)
        
        st = time.clock()
        for i in sorted_list:
            if i > 500000:
                break
            res = i
        print(res)
        print(time.clock() - st)
        
    binary_search_performance_test()