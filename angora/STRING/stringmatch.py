##encoding=utf-8

"""
Copyright (c) 2015 by Sanhe Hu
------------------------------
    Author: Sanhe Hu
    Email: husanhe@gmail.com
    Lisence: LGPL
    

Module description
------------------
    

Keyword
-------
    string
    
    
Compatibility
-------------
    Python2: Yes
    Python3: Yes


Prerequisites
-------------
    fuzzywuzzy


Import Command
--------------
    from angora.STRING.stringmatch import smatcher
"""

from __future__ import print_function
from fuzzywuzzy import process

class StrMatcher():
    def choose(self, text, choice, criterion = 75):
        res, score = process.extractOne(text, choice)
        if score >= criterion:
            return res
        else:
            return text
        
    def test_choose(self, text, choice):
        for pair in process.extract(text, choice):
            print(pair)

smatcher = StrMatcher()

if __name__ == "__main__":
    choice = ["Atlanta Falcons", "New Cow Jets", "Tom boy", "New York Giants", "Dallas Cowboys"]
    text = "cowboy"
    smatcher.choose(text, choice)
    smatcher.test_choose(text, choice)
    