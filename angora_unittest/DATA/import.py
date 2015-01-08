##encoding=utf8

from __future__ import print_function
from angora.DATA import *

def test_import():
    """
    from .dicttree import DictTree
    from .dtype import OrderedSet
    from .hashutil import md5_str, md5_obj, md5_file, hash_obj
    from .invertindex import invertindex
    from .iterable import flatten, flatten_all, nth, shuffled, grouper, grouper_dict, grouper_list
    from .iterable import running_windows, cycle_running_windows, cycle_slice
    from .js import load_js, dump_js, safe_dump_js, prt_js, js2str
    from .pk import load_pk, dump_pk, safe_dump_pk, obj2str, str2obj
    from .timewrapper import TimeWrapper
    """
    print(DictTree)
    print(OrderedSet)
    print(md5_str, md5_obj, md5_file, hash_obj)
    print(invertindex)
    print(flatten, flatten_all, nth, shuffled, grouper, grouper_dict, grouper_list)
    print(running_windows, cycle_running_windows, cycle_slice)
    print(load_js, dump_js, safe_dump_js, prt_js, js2str)
    print(load_pk, dump_pk, safe_dump_pk, obj2str, str2obj)
    print(TimeWrapper)
    
test_import()