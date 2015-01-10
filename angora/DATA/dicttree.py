##encoding=utf8

"""
dicttree is a tree structure implementation in python built-in dictionary

The dicttree is not a class,but actually a python dict. The way you manipulate it is through
a seriers of static method in DictTree. Because dict is a mutable object in python, so everything
you done inside the DictTree.some_method(dict) is really taking effect on it.

========================================= Definition ==============================================
dicttree concept

|---root = {"key": #key}
    |---a1 = {attr1: value1, attr2: value2}
        |---b1 = {attr1: value1, attr2: value2}
            |---c1 = {attr1: value1, attr2: value2}
            |---c2
            |---c3
            |---c4
        |---b2
        |---b3
    |---a2
    
definitions
    rootnode: the dict it self
    branch:   the set of it's child nodes
    node:     a #key : {"!!": attribute_dict} pair
    children: one of the child node on the branch 
    father:   father node
    level:    rootnode is on level 0, the child nodes of rootnode is on level 1, etc...

compatibility: compatible to python2 and python3

prerequisites: None

import:
    from angora.DATA.dicttree import DictTree
"""

from __future__ import print_function
from six import iterkeys, iteritems

class DictTree(object):
    @staticmethod
    def initial(key, **kwarg):
        """the root node has a special attribute "key".
        Because root node is the only dictionary doesn't have key. 
        So we assign the key as a special attribute.
        """
        d = dict()
        DictTree.setattr(d, key = key, **kwarg)
        return d
    
    @staticmethod
    def setattr(d, **kwarg):
        """set attributes is actually add a special key, value pair in this dict
            #key = "!!": #attributes_dict = {attribute_name: attribute_value}
        """
        if "!!" not in d:
            d["!!"] = dict()
        for k, v in kwarg.items():
            d["!!"][k] = v

    @staticmethod
    def getattr(d, attribute_name):
        """get attribute_value from the special attributes_dict
        """
        return d["!!"][attribute_name]
 
    @staticmethod
    def add_children(d, key, **kwarg):
        """Add a children with key and attributes. If children already EXISTS, OVERWRITE it.
        """
        if kwarg:
            d[key] = {"!!": kwarg}
        else:
            d[key] = dict()

    @staticmethod
    def ac(d, key, **kwarg):
        """abbreviation of self.add_children"""
        if kwarg:
            d[key] = {"!!": kwarg}
        else:
            d[key] = dict()
            
    @staticmethod
    def k(d):
        """equivalent of dict.keys() """
        return (key for key in iterkeys(d) if key != "!!")

    @staticmethod
    def v(d):
        """equivalent of dict.values() """
        return (value for key, value in iteritems(d) if key != "!!")

    @staticmethod
    def kv(d):
        """equivalent of dict.items() """
        return ((key, value) for key, value in iteritems(d) if key != "!!")

    @staticmethod
    def k_level(d, level, counter = 1):
        """iter keys on specific level
        level has to be greater equal than 0
        """
        if level == 0:
            yield d["!!"]["key"]
        else:
            if counter == level:
                for key in DictTree.k(d):
                    yield key
            else:
                counter += 1
                for node in DictTree.v(d):
                    for key in DictTree.k_level(node, level, counter):
                        yield key

    @staticmethod
    def v_level(d, level):
        """iter values on specific level
        level has to be greater equal than 0
        """
        if level == 0:
            yield d
        else:
            for node in DictTree.v(d):
                for node1 in DictTree.v_level(node, level-1):
                    yield node1

    @staticmethod
    def kv_level(d, level, counter = 1):
        """iter items on specific level
        level has to be greater equal than 0
        """
        if level == 0:
            yield d["!!"]["key"], d
        else:
            if counter == level:
                for key, node in DictTree.kv(d):
                    yield key, node
            else:
                counter += 1
                for node in DictTree.v(d):
                    for key, node in DictTree.kv_level(node, level, counter):
                        yield key, node

    @staticmethod   
    def length(d):
        """get the number of child nodes in this dict
        """
        if "!!" in d:
            return len(d) - 1
        else:
            return len(d)
    
    @staticmethod
    def len_on_level(d, level):
        """get the number of nodes on specific level in this dict
        """
        counter = 0
        for node in DictTree.v_level(d, level-1):
            counter += DictTree.length(node)
        return counter
    
    @staticmethod
    def stats_on_level(d, level):
        """print the node statistic info on specific level in this dict
        """
        num_of_emptynode, total = 0, 0
        for key, node in DictTree.kv_level(d, level):
            if DictTree.length(node) == 0:
                num_of_emptynode += 1
            total += 1
        print("On level %s, number of empty node = %s, total node = %s" % (level, 
                                                                           num_of_emptynode, 
                                                                           total))

    @staticmethod
    def del_level(d, level):
        """delete all the nodes on specific level in this dict
        """
        for node in DictTree.v_level(d, level-1):
            for key in [key for key in DictTree.k(node)]:
                del node[key]
                
if __name__ == "__main__":
    try:
        from .js import load_js, dump_js, safe_dump_js, prt_js, js2str
        from .pk import load_pk, dump_pk, safe_dump_pk, obj2str, str2obj
    except:
        from js import load_js, dump_js, safe_dump_js, prt_js, js2str
        from pk import load_pk, dump_pk, safe_dump_pk, obj2str, str2obj
    
    d = DictTree.initial("root")
    DictTree.setattr(d, pop = 299999999)
    DictTree.add_children(d, "VA", name = "virginia", population = 100000)
    DictTree.add_children(d, "MD", name = "maryland", population = 200000)
    
    DictTree.add_children(d["VA"], "arlington", name = "arlington county", population = 5000)
    DictTree.add_children(d["VA"], "vienna", name = "vienna county", population = 1500)
    DictTree.add_children(d["MD"], "bethesta", name = "montgomery country", population = 5800)
    DictTree.add_children(d["MD"], "germentown", name = "fredrick country", population = 1400)
    
    DictTree.add_children(d["VA"]["arlington"], "riverhouse", name = "RiverHouse 1400", population = 437)
    DictTree.add_children(d["VA"]["arlington"], "crystal plaza", name = "Crystal plaza South", population = 681)
    DictTree.add_children(d["VA"]["arlington"], "loft", name = "loft hotel", population = 216)
    
#     prt_js(d)
    
    def test1():
        """test for loop
        """
        print("{:=^100}".format("test for loop behavior"))
        global d
        
        print("\n{:=^60}".format("iter keys, behaive like dict.iterkeys"))
        for k in DictTree.k(d):
            print(k)
            
        print("\n{:=^60}".format("iter values, behaive like dict.itervalues"))
        for v in DictTree.v(d):
            print(v)
            
        print("\n{:=^60}".format("iter key, value pair, behaive like dict.iteritems"))
        for k, v in DictTree.kv(d):
            print(k, v)
            
#     test1()

    def test2():
        """test iter keys or values on specific level
        """
        global d
        
        print("\n{:=^60}".format("iter keys on specific depth level"))
        for key in DictTree.k_level(d, 0):
            print(key)      

        print("\n{:=^60}".format("iter values on specific depth level"))
        for node in DictTree.v_level(d, 0):
            print(node)

        print("\n{:=^60}".format("iter key and values on specific depth level"))
        for key, node in DictTree.kv_level(d, 3):
            print(key, node)

#     test2()

    def test3():
        """test length, and number of node on specific level
        """
        global d
        print(DictTree.length(d))                     # 2
        print(DictTree.length(d["VA"]))               # 2
        print(DictTree.length(d["VA"]["arlington"]))  # 3
        
        print(DictTree.len_on_level(d, 0)) # 0
        print(DictTree.len_on_level(d, 1)) # 2
        print(DictTree.len_on_level(d, 2)) # 4
        print(DictTree.len_on_level(d, 3)) # 3
        print(DictTree.len_on_level(d, 4)) # 0 there's no node depth = 4
        
        print(DictTree.len_on_level(d["VA"], 2)) # 3
        
        DictTree.stats_on_level(d, 0)
        DictTree.stats_on_level(d, 1)
        DictTree.stats_on_level(d, 2)
        DictTree.stats_on_level(d, 3)
        DictTree.stats_on_level(d, 4)
        
        print("VA" in d)
        print("arlington" in d["VA"])
    
#     test3()

    def test4():
        """delete behavior, delete the whole level
        """
        global d
        print("{:=^40}".format("before"))
        prt_js(d)
        DictTree.del_level(d, 2)
        print("{:=^40}".format("after"))
        prt_js(d)
        
#     test4()