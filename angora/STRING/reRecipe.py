##encoding=utf-8

"""
Copyright (c) 2015 by Sanhe Hu
------------------------------
    Author: Sanhe Hu
    Email: husanhe@gmail.com
    Lisence: LGPL


Module description
------------------
    This module is to make regular expression easier to use.
    With some built-in compiled pattern, we can use human language to generate
    re pattern.


Keyword
-------
    re, string

 
Compatibility
-------------
    Python2: Yes
    Python3: Yes


Prerequisites
-------------
    None
    
    
Import Command
--------------
    from angora.STRING.reRecipe import reparser
"""

from __future__ import print_function, unicode_literals
import re

class ReParser():
    """A advance regular expression extractor that have many useful built-in patterns.
    """
    def __init__(self):
        pass
    
    def extract_by_prefix_surfix(self, prefix, surfix, maxlen, text):
        """extract the text in between a prefix and surfix. you can name a max length
        """
        pattern = r"""(?<=%s)[\s\S]{1,%s}(?=%s)""" % (prefix, maxlen, surfix)
        return re.findall(pattern, text)

reparser = ReParser()

if __name__ == "__main__":
    import unittest
    
    class ReParserUnittest(unittest.TestCase):
        def test_extract_by_prefix_surfix(self):
            self.assertEqual(
                        reparser.extract_by_prefix_surfix(
                             "<div>", 
                             "</div>", 
                             100, 
                             "<a>中文<div>some text</div>英文</a>")[0],     
                        "some text"
                        )

    unittest.main()
