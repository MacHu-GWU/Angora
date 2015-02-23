##encodnig=UFT-8

from __future__ import print_function
from angora.LIBRARIAN import *

def set_initialize_mode_test():
    path = "windowsexplorer_test.py"
    
    wf = WinFile(path)
    print(wf.size_on_disk)
    
    # if we disable fast mode, then we don't have WinFile.size_on_disk attribute
    WinFile.set_initialize_mode(fastmode=True)
    wf = WinFile(path)
    print(wf.size_on_disk)
    
set_initialize_mode_test()