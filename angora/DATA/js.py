##encoding=utf8

"""

This module is re-pack of some json utility functions
    load_js
        load json object from file
        
    dump_js
        dump json object to file.
        
    safe_dump_js
        It's safe because that it dump to a temporary file first, then finally rename it.
        
    js2str
        convert dict to human readable string
    
    prt_js
        print dict in a human readable format with pretty indent
        
compatibility: compatible to python2 and python3

prerequisites: None

import:
    from angora.DATA.js import load_js, dump_js, safe_dump_js, js2str, prt_js 
"""

from __future__ import print_function
import json
import sys
import os, shutil
import time

is_py2 = (sys.version_info[0] == 2)
if is_py2:
    read_mode, write_mode = "rb", "wb"
else:
    read_mode, write_mode = "r", "w"
    
def load_js(fname, enable_verbose = True):
    """load dict object from file"""
    if enable_verbose:
        print("\nLoading from %s..." % fname)
        st = time.clock()
        
    if os.path.exists(fname): # exists, then load
        js = json.load(open(fname, read_mode) )
        if enable_verbose:
            print("\tComplete! Elapse %s sec." % (time.clock() - st) )
        return js
    
    else:
        if enable_verbose:
            print("\t%s not exists! cannot load! Create an empty dict instead" % fname)
        return dict()

def dump_js(js, fname, fastmode = False, replace = False, enable_verbose = True):
    """dump dict object to file.
    [Args]
    ------
    fname: save as file name
    
    fastmode: boolean, default False
        if True, then dumping json without sorting keys and pretty indent. It is faster
    
    replace: boolean, default False
        if True, when you dumping json to a existing file, it silently overwrite it.
        Default False setting is to prevent overwrite file by mistake
        
    enable_verbose: boolean, default True. Trigger for printing message
    """
    if enable_verbose:
        print("\nDumping to %s..." % fname)
        st = time.clock()
    
    if os.path.exists(fname): # if exists, check replace option
        if replace: # replace existing file
            if fastmode: # no sort and indent, do the fastest dumping
                json.dump(js, open(fname, write_mode))
            else:
                json.dump(js, open(fname, write_mode), sort_keys=True, indent=4,separators=("," , ": ") )
        else: # stop, print error message
            raise Exception("\tCANNOT WRITE to %s, it's already exists" % fname)
                
    
    else: # if not exists, just write to it
        if fastmode: # no sort and indent, do the fastest dumping
            json.dump(js, open(fname, write_mode))
        else:
            json.dump(js, open(fname, write_mode), sort_keys=True, indent=4,separators=("," , ": ") )
            
    if enable_verbose:
        print("\tComplete! Elapse %s sec" % (time.clock() - st) )

def safe_dump_js(js, fname, fastmode = False, enable_verbose = True):
    """
    [EN]Function dump_js has a fatal flaw. When replace = True, if the program is interrupted by 
    any reason. It only leave a incomplete file. (Because fully writing take time). And it silently
    overwrite the file with the same file name.
    
    1. dump json to a temp file.
    2. rename it to #fname, and overwrite it.
    
    [CN]dump_js函数在当replace=True时，会覆盖掉同名的文件。但是将编码后的字符串写入json是需要时间的，如果
    在这期间发生异常使程序被终止，那么会导致原来的文件已经被覆盖，而新文件还未完全被写入。这样会导致数据的
    丢失。
    safe dump js函数则是建立一个 前缀 + 文件名的临时文件，将json写入该文件中，当写入完全完成之后，将该文件
    重命名覆盖原文件。这样即使中途程序被中断，也仅仅是留下了一个未完成的临时文件而已，不会影响原文件。
    
    """
    temp_fname = "%s.tmp" % fname
    dump_js(js, temp_fname, fastmode = fastmode, replace = True, enable_verbose = enable_verbose)
    shutil.move(temp_fname, fname)
    
def js2str(js, sort_keys=True):
    """encode js to human readable string"""
    return json.dumps(js, sort_keys=sort_keys, indent=4, separators=("," , ": "))

def prt_js(js, sort_keys=True):
    """print dict object with pretty format"""
    print(js2str(js, sort_keys) )