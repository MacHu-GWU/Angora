##encoding=utf8

"""
一些winzip压缩的实用工具

compatibility: compatible to python2 and python3

prerequisites: None

import:
    from andora.LIBRARIAN.winzip import zip_a_folder, zip_everything_in_a_folder
"""

from __future__ import print_function
from zipfile import ZipFile
import os
 
def zip_a_folder(src, dst):
    """压缩整个文件夹
    """
    base_dir = os.getcwd()
     
    with ZipFile(dst, "w") as f:        
        dirname, basename = os.path.split(src)
        os.chdir(dirname)
        for dirname, _, fnamelist in os.walk(basename):
            for fname in fnamelist:
                newname = os.path.join(dirname, fname)
                f.write(newname)
         
    os.chdir(base_dir)
         
def zip_everything_in_a_folder(src, dst):
    """只压缩文件夹内部的文件和文件夹
    """
    base_dir = os.getcwd()
     
    with ZipFile(dst, "w") as f:
        os.chdir(src)
         
        for dirname, _, fnamelist in os.walk(os.getcwd()):
            for fname in fnamelist:
                newname = os.path.relpath(os.path.join(dirname, fname), 
                                          src)
                f.write(newname)
                 
    os.chdir(base_dir)
    
if __name__ == "__main__":
    zip_a_folder(r"C:\Python27\Scripts", "1.zip")
    zip_everything_in_a_folder(r"C:\Python27\Scripts", "2.zip")