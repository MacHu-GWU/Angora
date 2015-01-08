from angora.LIBRARIAN import *
import os

current_dir = os.getcwd()

fcs = FileCollections.from_path(current_dir)
fcs.print_files_size_greater_than(10 * 1000)

windir = WinDir(current_dir)
windir.prt_detail()
