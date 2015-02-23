##encoding=utf8

"""在python2中, 若路径有非ascii字符, 会带来许多错误, 所以本模组最好在python3环境中使用
本模组是对于windows中的directory和file系统, 提供了多种:
    对文件夹, 文件进行for循环的方法
    选择文件夹中的全部文件, 部分文件, 将选中的文件排序
    文件夹, 文件便捷重命名方法

compatibility: compatible to python2 and python3, BUT recommend python3 only

prerequisites: None

import
    from andora.LIBRARIAN.windowsexplorer import WinFile, WinDir, FileCollections, WinExplorer, string_SizeInBytes
"""

from __future__ import print_function
from collections import OrderedDict
import os

def string_SizeInBytes(size_in_bytes):
    """make size in bytes human readable. Doesn"t support size greater than 1TB
    """
    res, by = divmod(size_in_bytes,1024)
    res, kb = divmod(res,1024)
    res, mb = divmod(res,1024)
    tb, gb = divmod(res,1024)
    if tb != 0:
        human_readable_size = "%.2f TB" % (tb + gb/float(1024) )
    elif gb != 0:
        human_readable_size = "%.2f GB" % (gb + mb/float(1024) )
    elif mb != 0:
        human_readable_size = "%.2f MB" % (mb + kb/float(1024) )
    elif kb != 0:
        human_readable_size = "%.2f KB" % (kb + by/float(1024) )
    else:
        human_readable_size = "%s B" % by
    return human_readable_size

def chain_generator(top, down):
    """从top到down沿着路径依次生成路径名
    top = "C:\a"
    down = "C:\a\b\c\d"
    yield ["C:\a", "C:\b", "C:\c"]
    """
    top_dirname = os.path.split(top)[0]
    down_dirname = os.path.split(down)[0]
    if top in down:
        if top == down: # 如果两者相当, 则不返回任何
            pass
        else:
            chain = [top_dirname]
            for folder in os.path.relpath(down_dirname, top_dirname).split("\\"):
                chain.append(folder)
                yield os.path.join(*chain)
    else:
        raise Exception("%s must be children of %s" % (down, top))
    
def unittest_functions():
    top = r"C:\a"
    down1 = r"C:\a\b\c\d"
    down2 = r"C:\a"    
    down3 = r"C:\z\b\c\d"
    
    for i in chain_generator(top, down1):
        print(i)
    for i in chain_generator(top, down2):
        print(i)
    for i in chain_generator(top, down3): # 不能执行
        print(i)

# unittest_functions()

## ============== 类定义 ==============
class WinFile(object):
    """Windows文件对象, 可以通过 .属性名的方式访问 绝对路径, 文件夹路径, 文件名, 扩展名, 大小。
    免去了使用os.path.function的麻烦
    
    属性有:
        self.abspath    绝对路径              
        self.dirname    父文件夹路径          
        self.basename   完整文件名            
        self.fname      文件名
        self.ext        扩展名
        self.atime    最后一次触碰的时间
        self.ctime    文件被创建的时间
        self.mtime    文件最后一次被修改的时间
        
    The difference of: 
        access time (os.path.getatime)
        create time (os.path.getctime)
        modify time (os.path.getmtime)
        
    [EN]
        When rename, cut-and-paste, all 3 time stays.
        When edit the content, atime and mtime change, ctime stays.
        When copy the file to a new place, atime and ctime change, mtime stays.
    
    [CN]
        当文件被改名, 和剪切(剪切跟改名是一个操作), 所有3个时间都不变
        当文件内容被修改, atime, mtime变化, ctime不变
        当文件被复制到新地方时, atime, ctime变化, mtime不变
    """
    def __init__(self, abspath):
        if os.path.isfile(abspath): # 确保这是一个文件而不是目录
            self.abspath = os.path.abspath(abspath)
            self.initialize()
        else:
            raise Exception("%s is not a file or it doesn't exist." % abspath)

    def initialize(self):
        """method to initialize the value of some attributes
        """
        self.slow_initialize()
        
    def slow_initialize(self):
        """从绝对路径中获得:
        目录名, 文件名, 纯文件名, 文件扩展名
        文件大小, access time, create time, modify time
        预加载的内容较多, 速度较慢
        """
        self.dirname, self.basename = os.path.split(self.abspath) # 目录名, 文件名
        self.fname, self.ext = os.path.splitext(self.basename) # 纯文件名, 文件扩展名
        self.ext = self.ext.lower()
        
        self.size_on_disk = os.path.getsize(self.abspath)
        self.atime = os.path.getatime(self.abspath) # 接触时间
        self.ctime = os.path.getctime(self.abspath) # 创建时间, 当文件被修改后不变
        self.mtime = os.path.getmtime(self.abspath) # 修改时间

    def fast_initialize(self):
        """从绝对路径中获得:
        目录名, 文件名, 纯文件名, 文件扩展名
        预加载的内容较少, 速度较快
        """
        self.dirname, self.basename = os.path.split(self.abspath)
        self.fname, self.ext = os.path.splitext(self.basename)
    
    @staticmethod
    def set_initialize_mode(fastmode=False):
        """设置WinFile.initialize方法所绑定的初始化方式
        """
        if fastmode:
            WinFile.initialize = WinFile.fast_initialize
        else:
            WinFile.initialize = WinFile.slow_initialize
    
    def __str__(self):
        return self.abspath
    
    def __repr__(self):
        return self.abspath
    
    def rename(self, new_dirname = None, new_fname = None, new_ext = None):
        """对文件的目录名, 纯文件名进行重命名
        """
        if not new_dirname:
            new_dirname = self.dirname
        if not new_fname:
            new_fname = self.fname
        if new_ext: # 检查新文件名的扩展名格式是否
            if not new_ext.startswith("."):
                raise Exception("Extension must in format .ext, for example: .jpg, .mp3")
        else:
            new_ext = self.ext
            
        os.rename(self.abspath,
                  os.path.join(new_dirname, new_fname + new_ext) )
        # 如果成功重命名, 则更新文件
        self.fname, self.dirname, self.ext = new_fname, new_dirname, new_ext 
        
class WinDir(object):
    """Windows目录对象, 可以通过 .属性名来访问 绝对路径, 目录总大小, 子目录数量, 子文件数量。
    免去了使用os.path.function的麻烦。并提供了prt_detail()方法直接打印出文件夹的详细信息。
    
    属性有:
        size_total 文件夹总大小
        size_current_total 文件夹一级子文件总大小
        
        num_folder_total 子文件夹数量
        num_folder_current 一级子文件夹数量
        
        num_file_total 子文件数量
        num_file_current 一级子文件数量
    """
    def __init__(self, abspath):
        if os.path.isdir(abspath): # 确保这是一个目录而不是文件
            self.abspath = os.path.abspath(abspath)
            self.dirname, self.basename = os.path.split(self.abspath)
        else:
            raise Exception("%s is not a file." % abspath)

    def __str__(self):
        return self.abspath
    
    def __repr__(self):
        return self.abspath
    
    def get_detail(self):
        self.size_total = 0
        self.num_folder_total = 0
        self.num_file_total = 0
        
        self.size_current = 0
        self.num_folder_current = 0
        self.num_file_current = 0
        
        for current_dir, folderlist, fnamelist in os.walk(self.abspath):
            self.num_folder_total += len(folderlist)
            self.num_file_total += len(fnamelist)
            for fname in fnamelist:
                self.size_total += os.path.getsize(os.path.join(current_dir, fname))
                
        current_dir, folderlist, fnamelist = next(os.walk(self.abspath))
        self.num_folder_current = len(folderlist)
        self.num_file_current = len(fnamelist)
        for fname in fnamelist:
            self.size_current += os.path.getsize(os.path.join(current_dir, fname))
        
    def prt_detail(self):
        self.get_detail()
        screen = ["{:=^100}".format(" detail info of %s " % self.abspath),
                  "total size = %s" % string_SizeInBytes(self.size_total),
                  "number of sub folders = %s" % self.num_folder_total,
                  "number of total files = %s" % self.num_file_total,
                  "lvl 1 file size = %s" % string_SizeInBytes(self.size_current),
                  "lvl 1 folder number = %s" % self.num_folder_current,
                  "lvl 1 file number = %s" % self.num_file_current]
        print("\n".join(screen))

    def rename(self, new_basename = None, new_dirname = None):
        """对文件的目录名, 纯文件名进行重命名
        """
        if not new_basename:
            new_basename = self.new_basename
        if not new_dirname:
            new_dirname = self.dirname
        os.rename(self.abspath,
                  os.path.join(new_dirname, new_basename) )


class FileCollections():
    """WinFile的专用容器, 主要用于方便的从文件夹中选取文件, 筛选文件, 并对指定文件集排序。
    当然, 可以以迭代器的方式对容器内的文件对象进行访问。
    """
    def __init__(self):
        self.files = OrderedDict() # {文件绝对路径: 包含各种详细信息的WinFile对象}
    
    def __str__(self):
        if len(self.files) == 0:
            return "***Empty FileCollections***"
        try:
            return "\n".join(list(self.order))
        except:
            return "\n".join(list(self.files.keys()))
    
    def __len__(self):
        return len(self.files)
    
    def __contains__(self, path):
        if os.path.abspath(path) in self.files:
            return True
        else:
            return False
        
    def howmany(self):
        return len(self.files)
    
    def iterfiles(self):
        """yield all WinFile object"""
        try:
            for path in self.order:
                yield self.files[path]
        except:
            for winfile in self.files.values():
                yield winfile
                
    def iterpaths(self):
        """yield all WinFile's absolute path"""
        try:
            for path in self.order:
                yield path
        except:
            for path in self.files:
                yield path
    
    def __iter__(self):
        """default iterator is to yield absolute paht only"""
        return self.iterpaths()
    
    def sort_by(self, attr_name, reverse = False):
        """
        [EN]sort WinFile by specific WinFile.attributes
        [CN]对容器内的WinFile根据其某一个属性升序或者降序排序
        """
        try:
            d = dict()
            for abspath, winfile in self.files.items():
                d[abspath] = getattr(winfile, attr_name)
            self.order = list(OrderedDict( sorted(list(d.items()), 
                                                  key=lambda t: t[1],
                                                  reverse = reverse) ).keys())
        except AttributeError:
            raise Exception("""valid sortable attributes are [abspath, dirname, basename, fname, ext,
            size_on_disk, atime, ctime, mtime];""")

    def select_all_from_path(self, path):
        """从path目录下选择所有文件, 并填入self.files中
        """
        for abspath in WinExplorer._iterfiles(path):
            self.files[abspath] = WinFile(abspath)
            
    def select(self, criterion, keepboth = False):
        """
        [EN]
        Select WinFile from current container by the definition of the criterion function, and return
        a new FileCollections. If parameter keepboth = True, then return two FileCollections.
        First one matches the criterion, second one doens't.
        
        [CN]
        从当前容器中的文件里, 根据criterion中的规则, 选取符合条件的WinFile, 打包成新的FileCollections返回。
        当keepboth参数=True时, 返回两个FileCollections, 一个是符合条件的, 一个是不符合条件的
        """
        if keepboth:
            fcs_yes, fcs_no = FileCollections(), FileCollections()
            for winfile in self.files.values():
                if criterion(winfile):
                    fcs_yes.files[winfile.abspath] = winfile
                else:
                    fcs_no.files[winfile.abspath] = winfile
            return fcs_yes, fcs_no
        else:
            fcs = FileCollections()
            for winfile in self.files.values():
                if criterion(winfile):
                    fcs.files[winfile.abspath] = winfile
            
            return fcs

    @staticmethod
    def from_path(path):
        """直接选取path目录下所有文件, 并生成一个FileCollections
        """
        fcs = FileCollections()
        for abspath in WinExplorer._iterfiles(path):
            fcs.files[abspath] = WinFile(abspath)
        return fcs
    
    @staticmethod
    def from_path_by_criterion(path, criterion, keepboth = False):
        """直接选取path目录下所有文件, 根据criterion中的规则, 生成FileCollections
        """
        if keepboth:
            fcs_yes, fcs_no = FileCollections(), FileCollections()
            for abspath in WinExplorer._iterfiles(path):
                winfile = WinFile(abspath)
                if criterion(winfile):
                    fcs_yes.files[winfile.abspath] = winfile
                else:
                    fcs_no.files[winfile.abspath] = winfile
            return fcs_yes, fcs_no
        else:
            fcs = FileCollections()
            for abspath in WinExplorer._iterfiles(path):
                winfile = WinFile(abspath)
                if criterion(winfile):
                    fcs.files[winfile.abspath] = winfile
            return fcs
    
    ### useful recipe
    def print_files_size_greater_than(self, threshold):
        def bigfile_criterion(winfile):
            if winfile.size_on_disk >= threshold:
                return True
            else:
                return False

        fcs = self.select(bigfile_criterion)
        fcs.sort_by("size_on_disk")
        for winfile in fcs.iterfiles():
            print("%s - %s" % (string_SizeInBytes(winfile.size_on_disk), winfile))
        print("\tAbove is all files size greater than %s." % string_SizeInBytes(threshold))
        
        
class WinExplorer(object):
    """Windows文件浏览器, 提供了
    iterfolders, iterfiles, itertopfolders, itertopfiles等方法方便地对文件夹或文件进行遍历
    """
    def __init__(self):
        self.focus = None
        
    def locate(self, path):
        """定位到某一个文件夹
        """
        if os.path.isdir(path):
            if os.path.exists(path):
                self.focus = os.path.abspath(path)
            else:
                raise Exception("%s not exists" % path)
        else:
            raise Exception("%s may not exists or is not a directory!" % path)
    
    def scan_all(self):
        """对当前定位到的文件夹中的目录和文件进行扫描
        """
        dir_collections = dict()
        file_collections = dict()
        
        for current_dir, folderlist, fnamelist in os.walk(self.focus):
            
            for folder in folderlist:
                windir = WinDir(os.path.join(current_dir, folder))
                dir_collections[windir.abspath] = windir
                
            for fname in fnamelist:
                winfile = WinFile(os.path.join(current_dir, fname))
                file_collections[winfile.abspath] = winfile
        
        self.dir_collections = dir_collections
        self.file_collections = file_collections
        
    def scan_file(self):
        """对当前定位到的文件夹中的文件进行扫描
        """
        file_collections = dict()
        for path in self.iterfiles():
            winfile = WinFile(path)
            file_collections[winfile.abspath] = winfile
        self.file_collections = file_collections
        
    def scan_dir(self):
        """对当前定位到的文件夹中的目录进行扫描
        """
        dir_collections = dict()
        for path in self.iterfolders():
            windir = WinDir(path)
            dir_collections[windir.abspath] = windir
        self.dir_collections = dir_collections
    
    ### iterator function
    @staticmethod
    def _iterfolders(path):
        """遍历path目录下的所有子目录
        """
        if os.path.isdir(path):
            for current_folder, folderlist, _ in os.walk(path):
                for folder in folderlist:
                    yield os.path.join(current_folder, folder)
        else:
            raise Exception("%s may not exists or is not a directory!" % path)

    def iterfolders(self, path = None):
        """对path下的所有目录的绝对路径进行遍历, 若path不存在, 则使用当前所在文件夹
        """
        if not path:
            path = self.focus
        return self._iterfolders(path)

    @staticmethod
    def _iterfiles(path):
        """遍历path目录下的所有文件
        """
        if os.path.isdir(path):
            for current_folder, _, fnamelist in os.walk(path):
                for fname in fnamelist:
                    yield os.path.join(current_folder, fname)
        else:
            raise Exception("%s may not exists or is not a directory!" % path)

    def iterfiles(self, path = None):
        """对path下的所有文件的绝对路径进行遍历, 若path不存在, 则使用当前所在文件夹
        """
        if not path:
            path = self.focus
        return self._iterfiles(path)
        
    @staticmethod
    def _itertopfolders(path):
        """遍历path目录下的所有1级子目录
        """
        if os.path.isdir(path):
            current_folder, folderlist, _ = next(os.walk(path))
            for folder in folderlist:
                yield os.path.join(current_folder, folder)
        else:
            raise Exception("%s may not exists or is not a directory!" % path)
        
    def itertopfolders(self, path = None):
        """对path下的所有1级目录的绝对路径进行遍历, 若path不存在, 则使用当前所在文件夹
        """
        if not path:
            path = self.focus
        return self._itertopfolders(path)
    
    @staticmethod
    def _itertopfiles(path):
        """遍历path目录下的所有1级文件
        """
        if os.path.isdir(path):
            current_folder, _, fnamelist = next(os.walk(path))
            for fname in fnamelist:
                yield os.path.join(current_folder, fname)
        else:
            raise Exception("%s may not exists or is not a directory!" % path)

    def itertopfiles(self, path = None):
        """对path下的所有1级文件的绝对路径进行遍历, 若path不存在, 则使用当前所在文件夹
        """
        if not path:
            path = self.focus
        return self._itertopfiles(path)
        
    ### useful receipt
    def create_fake_mirror(self, src, dst):
        """copy all dir, files from src to dst. But only create a empty file with same file name.
        However, the tree structure doesn't change.
        """
        if not (os.path.exists(src) and (not os.path.exists(dst)) ):
            raise Exception("source not exist or distination already exist")
        self.locate(src)
        self.scan_file()
        
        os.mkdir(dst)
        for winfile in self.file_collections.values():
            try:
                os.makedirs(os.path.join(dst, os.path.relpath(winfile.dirname, src)))
            except:
                pass
            with open(os.path.join(dst, os.path.relpath(winfile.abspath, src)), "w") as _:
                pass
    
if __name__ == "__main__":
    def unittest_winfile():
        wf = WinFile("windowsexplorer.py")
        print(wf)
        
    # unittest_winfile()

    def unittest_WinDir():
        wd = WinDir(r"C:\HSH\PythonWorkspace\py3\py33_projects\Angora")
        wd.prt_detail()
        
    # unittest_WinDir()

    def unittest_FileCollections():
        entrance = r"C:\Users\shu\Documents\PythonWorkSpace\py3\py33_projects\Angora"
        
        def pythonfile_criterion(winfile): # 定义python文件筛选器
            if winfile.ext in [".py"]:
                return True
            else:
                return False
            
        ### 测试类的基本功能
        def basic_test():    
            fcs = FileCollections()
            fcs.select_all_from_path(entrance) # 选择所有文件
            print(fcs.howmany())
    
            fcs1 = fcs.select(pythonfile_criterion) # 从中选择python文件
            print(fcs1)
            print(fcs1.howmany())
        
        # basic_test()
        
        ### 测试简便的静态方法
        def static_method_test():
            fcs_from_folder = FileCollections.from_path(entrance)
            print("{:=^100}".format("fcs_from_folder"))
            print(fcs_from_folder.howmany())
            
            fcs_from_folder_yes, fcs_from_folder_no = FileCollections.from_path_by_criterion(entrance, 
                                                                                             pythonfile_criterion, 
                                                                                             True)
            print("{:=^100}".format("fcs_from_folder_yes"))
            print(fcs_from_folder_yes.howmany())
            print("{:=^100}".format("fcs_from_folder_no"))
            print(fcs_from_folder_no.howmany())
        
        # static_method_test()
        
        ### 测试使用功能
        def recipe_test():
            fcs = FileCollections.from_path(entrance)
            fcs.print_files_size_greater_than(1000)
        
        # recipe_test()
        
    # unittest_FileCollections()


    def unittest_WinExplorer():
        entrance = r"C:\Users\shu\Documents\PythonWorkSpace\py3\py33_projects\Angora"
        we = WinExplorer()
        we.locate(entrance)
        
        def basic_test():
            print("{:=^100}".format("test scan function"))
            we.scan_all() # 扫描目录下的所有文件和目录
            we.scan_file() # 仅扫描文件
            print(we.file_collections)
            we.scan_dir() # 仅扫描目录
            print(we.dir_collections)
        
        # basic_test()
        
        ### 测试各种迭代器
        def iterator_method_test():
            print("{:=^100}".format("test staticmethod iter all folders"))
            for path in WinExplorer._iterfolders(entrance):
                print(path)
            print("{:=^100}".format("test iter all folders"))
            for path in we.iterfolders():
                print(path)
            print("{:=^100}".format("test staticmethod iter all files"))
            for path in WinExplorer._iterfiles(entrance):
                print(path)
            print("{:=^100}".format("test iter all files"))
            for path in we.iterfiles():
                print(path)
    
            print("{:=^100}".format("test staticmethod iter all top folders"))
            for path in WinExplorer._itertopfolders(entrance):
                print(path)
            print("{:=^100}".format("test iter all top folders"))
            for path in we.itertopfolders():
                print(path)
            print("{:=^100}".format("test staticmethod iter all top files"))
            for path in WinExplorer._itertopfiles(entrance):
                print(path)
            print("{:=^100}".format("test iter all top files"))
            for path in we.itertopfiles():
                print(path)
    
        # iterator_method_test()
        
        def create_fake_mirror_test(): # 测试文件夹伪拷贝功能
            src = r"C:\Users\shu\Documents\PythonWorkSpace\py3\py33_projects\Angora"
            dst = r"C:\Users\shu\Documents\PythonWorkSpace\py3\py33_projects\Angora_fake"
            we.create_fake_mirror(src, dst)
            
        # create_fake_mirror_test()

    # unittest_WinExplorer()