##encoding=UTF-8

from __future__ import print_function
from angora.LIBRARIAN import *

def test_zip_a_folder():
    zip_a_folder(r"C:\Python27\Scripts", "1.zip")
    
test_zip_a_folder()

def test_zip_everything_in_a_folder():
    zip_everything_in_a_folder(r"C:\Python27\Scripts", "2.zip")
    
test_zip_everything_in_a_folder()

def test_zip_many_files():
    def pythonfile_filter(winfile):
        if winfile.ext == ".py":
            return True
        else:
            return False
    fc = FileCollections.from_path_by_criterion("C:\HSH\Python34_workspace\Python34_Projects\Angora", pythonfile_filter)
    zip_many_files(fc.iterpaths(), "test.zip")
    
test_zip_many_files()

    

