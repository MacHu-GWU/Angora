##encoding=UTF8

"""
Messenger
---------
    一个print消息管理小工具
    能很轻易地将所有print函数禁用和激活, 而无需修改和注释大量代码
    
Configuration
-------------

compatibility: compatible to python2, python3

prerequisites: None
    
import:
    from config import Configuration
"""

from __future__ import print_function

class Configuration(object):
    """Configuration is a config data handler class. A standard config.json file is a simple
    key-value dictionary. You can visit value by calling Configuration.key. All keys have to 
    be str, and no space is allowed.
    Valid Configuration json file example:
        {"key1": value1, "key2": value2, ...}
        
    Usually, Configuration use as a read only object. So there's no method to dump it to a json.
    
    注意!!!, value中不能 '\' 这个转移字符, 如果参数必须包含'\', 则Configuration这个办法不适用。
    """
    @staticmethod
    def read(config_json_file_path):
        """create a Configuration instance by reading data from a config json file
        you can visit value via Configuration.key_name
        """
        config = Configuration()
        with open("config.json", "rb") as f: # 读取文件内容
            content = f.read().decode("utf8") # 避免乱码的出现, 得到字符串的content
        
        lines = list()
        for line in content.split("\n"):
            index = line.find("//") # 找到注释的标识
            if index == -1: # 如果没有注释
                lines.append(line) # 直接append
            else: # 如果有注释
                lines.append(line[:index]) # 删掉注释的部分

        content = "\n".join(lines) # 从新创建一个无注释的content
        for k, v in eval(content).items(): # 把信息存入字典
            config.__setattr__(k, v)
        return config

    def print_all_key_value(self):
        """convenient method to print all key-value
        """
        for key, value in self.__dict__.items():
            print("%s: %s" % (repr(key), repr(value)))
            
if __name__ == "__main__":
    import unittest
    import os
    class ConfigurationUnittest(unittest.TestCase):
        def setUp(self):
            # create json file
            content = \
            """
            {
                "key1": "value1", // this is a comment
                "key2": [ // 测试 
                    100,
                    "中文",
                    ]
            }
            """
            with open("config.json", "wb") as f:
                f.write(content.encode("utf8").strip())
            self.config = Configuration.read("config.json")
            
        def test_read(self):
            self.assertEqual(self.config.key1, "value1")
            self.assertEqual(self.config.key2[0], 100)
            self.assertEqual(self.config.key2[1], "中文")
            
        def tearDown(self):
            os.remove("config.json")
            
    unittest.main()