##encoding=utf-8

"""
Import Command
--------------
    from util.htmlparser import htmlparser
"""

from bs4 import BeautifulSoup as BS4

class HTMLParser():
    def __init__(self):
        pass
    
    
htmlparser = HTMLParser()

if __name__ == "__main__":
    import unittest
    
    def read(abspath, encoding="utf-8"):
        with open(abspath, "rb") as f:
            return f.read().decode(encoding)
    
    class HTMLParserUnittest(unittest.TestCase):
        pass
    
    unittest.main()