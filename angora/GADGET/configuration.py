##encoding=UTF8

from __future__ import print_function
from angora.DATA.dtype import OrderedSet

class Configuration():
    def __init__(self):
        self._sections = OrderedSet(["DEFAULT"])
        self.DEFAULT = dict()
        
    def add_section(self, section_name):
        if section_name == "DEFAULT":
            raise Exception("'DEFAULT' is reserved section name.")
        
        if section_name in self._sections:
            raise Exception("Error! %s is already one of the sections" % section_name)
        else:
            object.__setattr__(self, section_name, dict())
            self._sections.add(section_name)
            
    def remove_section(self, section_name):
        if section_name == "DEFAULT":
            raise Exception("'DEFAULT' is reserved section name.")
        
        if section_name in self._sections:
            delattr(self, section_name)
            self._sections.discard(section_name)
        else:
            raise Exception("Error! %s is not any of the sections" % section_name)
        
    def sections(self):
        section_set = OrderedSet(list(self._sections))
        section_set.discard("DEFAULT")
        return list(section_set)
    
    def dump(self, path):
        pass
    
    
    
    
    
    
    
    
    
    
if __name__ == "__main__":
    import unittest
    
    class ConfigurationUnittest(unittest.TestCase):
        def setUp(self):
            """ftp server
            """
            self.config = Configuration()
            self.config.DEFAULT["localhost"] = "192.168.0.1"
            self.config.DEFAULT["port"] = 8080
            self.config.DEFAUL["connection_timeout"] = 60 # seconds
            
            self.config.add_section("UPLOAD")
            self.config.UPLOAD["sizelimit"] = 1024 * 1024 * 1024
            self.config.add_section("DOWNLOAD")
            self.config.DOWNLOAD[""] 
            
            
            
    config = Configuration()
    config.add_section("default")
    
    print(config.default)
    
    default = config.default
    default["a"] = 1
    print(config.default)
    
    unittest.main()