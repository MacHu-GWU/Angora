##encoding=utf-8

"""
Copyright (c) 2015 by Sanhe Hu
------------------------------
    Author: Sanhe Hu
    Email: husanhe@gmail.com
    Lisence: LGPL

Module description
------------------
    Messenger
        一个print消息管理小工具
        能很轻易地将所有print函数禁用和激活, 而无需修改和注释大量代码
    
    Log
        一个简单的日志文件夹和日志文件的管理小工具
        能很轻易的把捕获到的异常和自定义错误信息写入到日志中, 并在屏幕上以自定义的缩进形式将错误信息打印出来。
        用户可以设定写入日志时是否同时打印到屏幕上, 并可以自定义缩进大小。
    
        建议在用到try, except语法时, 在except语法块中使用:
            log.write(message, index, indent, enable_verbose)
    
Keyword
-------
    log
    
    
Compatibility
-------------
    Python2: Yes
    Python3: Yes


Prerequisites
-------------
    None


Import Command
--------------
    from angora.GADGET.logger import Messenger, messenger, Log
"""

from __future__ import print_function
import datetime
import os
            
class Messenger():
    """Messenger is an utility to easily disable or enable all your print function
    [EN]
        Sometime you may have a lots of print("something") in your script. But how about if you want
        to disable them all or part of them? Usually we have to comment them all. But Messenger
        provide Messenger.off() and Messenger.on() to easily disable/enable all Messenger.show()
        functionality.
        
        Usage:
            use Messenger.show("something") to replace all your print("something")
            
            Messenger.off() # this will disable all Messenger.show()
            
            Messenger.on() # this will enable all Messenger.show()
            
    [CN]
        在python程序中为了调试方便, 我们通常会有大量的print()。但是如果我们想要一次性禁用大量的print(), 我们
        就需要很麻烦的注释掉许多print()。Messenger提供了一种方法解决这一问题。
        
        每当我们想要用print()的时候, 我们可以使用Messenger.show("something")
        但是我们想要一次性禁用所有的print(), 我们只需要调用Messenger.off()即可。同样如果需要恢复打印功能, 
        我们只需要调用Messenger.on()即可
    """
    def __init__(self, enable_verbose=True):
        """echo=False to disable all Messenger.show()
        """
        self.enable_verbose = enable_verbose
        if self.enable_verbose:
            self.show = self._print_screen
        else:
            self.show = self._not_print_screen
            
    def _print_screen(self, text):
        print(text)
        
    def _not_print_screen(self, text):
        pass

    def on(self):
        """enable Messenger.show()"""
        self.show = self._print_screen
        
    def off(self):
        """disable Messenger.show()"""
        self.show = self._not_print_screen

messenger = Messenger()

class Log(object):
    """
    [CN]usage
        当初始化Log类的时候，会自动在脚本运行所在目录创建一个叫log的目录
        每当try，except时，在人类能预计错误的情况下，可以用
        Log.write(index, message)方法把日志写入名为%Y-%m-%d %H_%M_%S.txt的
        日志文件中。（index和message就可以自己定义了）
        而在无法预知错误的情况下，可以用：
        import sys
        Log.write(index = sys.exc_info()[0], message = sys.exc_info()[1])
        把捕获到的异常写入日志
    """
    def __init__(self, directory = "log"):
        """
        [EN]Automatically create a folder named "log" within the same folder with the main script
        [CN]自动在调用此包的主脚本下创建一个"log"的文件夹，可以在初始化时修改
        """
        self.fname = "%s.txt" % datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H_%M_%S")
        self.directory = os.path.abspath(directory)
        if not os.path.exists(self.directory):
            os.mkdir(self.directory)

    def write(self, message, index = "unknown", indent = 1, enable_verbose = True):
        """
        [EN]Write line to local log file. And automatically print log info  
        [CN]把异常信息写入日志，并自动打印出来
        
        log line format:
            <datetime><index><message>        

        [Args]
        ------
        message: log text message
        index: type of this log
        indent: how many indent added to the message head when printing
        enable_verbose: print message while writing to log or not
        """
        line = "<%s><%s><%s>\n" % (datetime.datetime.now(),
                                   index,
                                   message)
        with open(os.path.join(self.directory, self.fname), "a") as f:
            f.write(line)
        if enable_verbose:
            print("%s<%s><%s>" % ("\t" * indent, index, message) ) # print log info
    
if __name__ == "__main__":
    def Messenger_test():
        messenger = Messenger()
        messenger.show("hello world")
        messenger.off()
        messenger.show("hello world")
        messenger.on()
        messenger.show("hello world")
        
    Messenger_test()        