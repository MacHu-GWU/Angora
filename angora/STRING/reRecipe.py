##encoding=UTF8

"""
author: Sanhe Hu

compatibility: python2, python3 

prerequisites: None

import:
    from angora.STRING.reRecipe import ReParser
"""

from __future__ import print_function, unicode_literals
import re

class ReParser():
    """A advance regular expression parser which has many useful built-in patterns.
    """
    def __init__(self):
        pass
    
    def extract_by_prefix_surfix(self, prefix, surfix, maxlen, text):
        """extract the text in between a prefix and surfix. you can name a max length
        """
        pattern = r"""(?<=%s)[\s\S]{1,%s}(?=%s)""" % (prefix, maxlen, surfix)
        return re.findall(pattern, text)
      
if __name__ == "__main__":
    import unittest
    
    class ReParserUnittest(unittest.TestCase):
        def test_extract_by_prefix_surfix(self):
            reparser = ReParser()
            self.assertEqual(
                        reparser.extract_by_prefix_surfix(
                             "<div>", 
                             "</div>", 
                             100, 
                             "<a>中文<div>some text</div>英文</a>")[0],     
                        "some text"
                        )

    unittest.main()

