##encoding=utf8

"""
import:
    from angora.STRING.stringmatch import StrMatcher
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
        
    def choose_test(self, text, choice):
        for pair in process.extract(text, choice):
            print(pair)
        
if __name__ == "__main__":
    matcher = StrMatcher()
    
    choice = ["Atlanta Falcons", "New Cow Jets", "Tom boy", "New York Giants", "Dallas Cowboys"]
    
    text = "cowboy"
#     matcher.choose(text, choice)
    matcher.choose_test(text, choice)
    