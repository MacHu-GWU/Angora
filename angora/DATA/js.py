##encoding=utf-8

"""
Copyright (c) 2015 by Sanhe Hu
------------------------------
    Author: Sanhe Hu
    Email: husanhe@gmail.com
    Lisence: LGPL
    

Module description
------------------
    This module is re-pack of some json utility functions
        load_js
            load json object from file
            
        dump_js
            dump json object to file.
            
        safe_dump_js
            It's safe because that it dump to a temporary file first, then finally rename it.
            Which prevent in-complete file caused by interrupted while writing
            
        js2str
            stringlize dictionary to human readable string
        
        prt_js
            print dictionary in a human readable format with pretty indent
        
        
Keyword
-------
    json, IO
    
    
Compatibility
-------------
    Python2: Yes
    Python3: Yes
    
    
Prerequisites
-------------
    None


Import Command
--------------
    from angora.DATA.js import load_js, dump_js, safe_dump_js, js2str, prt_js 
    
"""

from __future__ import print_function, unicode_literals
import json
import os, shutil
import time

def load_js(abspath, enable_verbose = True):
    """load json from file"""
    if enable_verbose:
        print("\nLoading from %s..." % abspath)
        st = time.clock()
        
    if os.path.exists(abspath): # exists, then load
        with open(abspath, "r") as f:
            js = json.load(f)
        if enable_verbose:
            print("\tComplete! Elapse %s sec." % (time.clock() - st) )
        return js
    
    else:
        if enable_verbose:
            print("\t%s not exists! cannot load! Create an empty dict instead" % abspath)
        return dict()

def dump_js(js, abspath, fastmode = False, replace = False, enable_verbose = True):
    """dump dict object to file.
    [Args]
    ------
    abspath: save as file name
    
    fastmode: boolean, default False
        if True, then dumping json without sorting keys and pretty indent. It is faster
    
    replace: boolean, default False
        if True, when you dumping json to a existing file, it silently overwrite it.
        Default False setting is to prevent overwrite file by mistake
        
    enable_verbose: boolean, default True. Trigger for printing message
    """
    if enable_verbose:
        print("\nDumping to %s..." % abspath)
        st = time.clock()
    
    if os.path.exists(abspath): # if exists, check replace option
        if replace: # replace existing file
            if fastmode: # no sort and indent, do the fastest dumping
                with open(abspath, "w") as f:
                    json.dump(js, f)
            else:
                with open(abspath, "w") as f:
                    json.dump(js, f, sort_keys=True, indent=4,separators=("," , ": ") )
        else: # stop, print error message
            raise Exception("\tCANNOT WRITE to %s, it's already exists" % abspath)
                
    
    else: # if not exists, just write to it
        if fastmode: # no sort and indent, do the fastest dumping
            with open(abspath, "w") as f:
                json.dump(js, f)
        else:
            with open(abspath, "w") as f:
                json.dump(js, f, sort_keys=True, indent=4,separators=("," , ": ") )
            
    if enable_verbose:
        print("\tComplete! Elapse %s sec" % (time.clock() - st) )

def safe_dump_js(js, abspath, fastmode = False, enable_verbose = True):
    """
    [EN]Function dump_js has a fatal flaw. When replace = True, if the program is interrupted by 
    any reason. It only leave a incomplete file. (Because fully writing take time). And it silently
    overwrite the file with the same file name.
    
    1. dump json to a temp file.
    2. rename it to #abspath, and overwrite it.
    
    [CN]dump_js函数在当replace=True时，会覆盖掉同名的文件。但是将编码后的字符串写入json是需要时间的，如果
    在这期间发生异常使程序被终止，那么会导致原来的文件已经被覆盖，而新文件还未完全被写入。这样会导致数据的
    丢失。
    safe dump js函数则是建立一个 前缀 + 文件名的临时文件，将json写入该文件中，当写入完全完成之后，将该文件
    重命名覆盖原文件。这样即使中途程序被中断，也仅仅是留下了一个未完成的临时文件而已，不会影响原文件。
    
    """
    temp_abspath = "%s.tmp" % abspath
    dump_js(js, temp_abspath, fastmode = fastmode, replace = True, enable_verbose = enable_verbose)
    shutil.move(temp_abspath, abspath)
    
def js2str(js, sort_keys=True):
    """encode js to human readable string"""
    return json.dumps(js, sort_keys=sort_keys, indent=4, separators=("," , ": "))

def prt_js(js, sort_keys=True):
    """print dict object with pretty format"""
    print(js2str(js, sort_keys) )
    
############
# Unittest #
############

if __name__ == "__main__":
    import unittest
    
    class JSUnittest(unittest.TestCase):
        def test_write_and_read(self):
            data = {"a": [1, 2], "b": ["是", "否"]} 
            safe_dump_js(data, "data.json")
            data = load_js("data.json")
            self.assertEqual(data["a"][0], 1)
            self.assertEqual(data["b"][0], "是")
            
        def test_js2str(self):
            data = {"a": [1, 2], "b": ["是", "否"]} 
            prt_js(data)
        
        def tearDown(self):
            try:
                os.remove("data.json")
            except:
                pass
            
    unittest.main()