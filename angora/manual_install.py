##encoding=UTF8

"""
[En]If you run this file as the main script.
    Then package will be installed for all Python version you have installed
[Cn]将本脚本作为主脚本运行，会把本脚本所在的package安装到所有用户已安装的python版本的
    site-packages下。不支持需要C预编译文件的库。
"""

from __future__ import print_function

if __name__ == "__main__":
    def install():
        """
        This script is to install the package to all installed python version
        """
        import os, shutil
        # get installed python information
        py2_folders, py3_folders = list(), list()
        for _, folders, _ in os.walk(r"C:\\"):
            for folder in folders:
                if folder.startswith("Python2"):
                    py2_folders.append(folder)
                elif folder.startswith("Python3"):
                    py3_folders.append(folder)
                    
            print("For Py2x You have installed %s." % py2_folders)
            print("For Py3x You have installed %s." % py3_folders)
            break
        
        # remove existing temporary files
        print("\nRemoving existing __pycache__ folder and .pyc files")
        folder_to_be_delete, fname_to_be_delete = list(), list()
        for root, folders, fnames in os.walk(os.getcwd()):
            if os.path.basename(root) == "__pycache__": # if is cache folder
                folder_to_be_delete.append(root) # add to to-delete list
            for fname in fnames:
                if fname.endswith(".pyc"): # if is pre-compile file
                    fname_to_be_delete.append(os.path.join(root, fname)) # add to to-delete list
        
        for folder in folder_to_be_delete:
            shutil.rmtree(folder)
        for fname in fname_to_be_delete:
            try:
                os.remove(fname)
            except:
                pass
            
        # remove currently installed packages and 
        # copy file to site-packages in all python versions
        for pyroot in py2_folders + py3_folders:
            dst = os.path.join("C:\\", pyroot, 
                               r"Lib\site-packages", 
                               os.path.basename(os.getcwd() ) )
            try: # remove
                print("Deleting %s" % dst)
                shutil.rmtree(dst)
            except:
                pass
             
            print("Copying file to %s..." % dst) # copy to
            shutil.copytree(os.path.abspath(os.getcwd() ), 
                            dst)

    install()