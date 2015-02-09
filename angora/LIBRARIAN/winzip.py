##encoding=utf8

"""
一些winzip压缩的实用工具

compatibility: compatible to python2 and python3

prerequisites: None

import:
    from andora.LIBRARIAN.winzip import zip_a_folder, zip_everything_in_a_folder, zip_many_files

注: python中zipfile包自带的ZipFile方法的用法如下
    基本用法:
        with ZipFile("filename.zip", "w") as f:
            f.write(path)
        其中path是文件路径。 如果path是文件夹, 并不会将文件夹内所有的文件添加到压缩包中。
    
    相对路径压缩:
        比如你有一个路径C:\download\readme.txt
        如果当前路径是C:\, 而此时你将readme.txt添加到压缩包时则是在压缩包内添加一个:
            download\readme.txt
        如果当前路径是C:\download\, 则在压缩包内添加的路径则是:
            readme.txt
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
    
def zip_many_files(list_of_abspath, dst):
    """将一系列的文件压缩到一个压缩包中, 若有重复的文件名, 在zip中保留所有的副本
    """
    base_dir = os.getcwd()
    
    with ZipFile(dst, "w") as f:
        for abspath in list_of_abspath:
            dirname, basename = os.path.split(abspath)
            os.chdir(dirname)
            f.write(basename)
    
    os.chdir(base_dir)
    
if __name__ == "__main__":
    zip_a_folder(r"C:\Python27\Scripts", "1.zip")
    zip_everything_in_a_folder(r"C:\Python27\Scripts", "2.zip")