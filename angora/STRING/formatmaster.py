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
    None


Import Command
--------------
    from angora.STRING.formatmaster import fmter
"""

from __future__ import print_function
import random

class Template():
    """
    [EN]generate string from built-in templates. method start with '_' means print the string
    instead of returnning it.
    [CN]用于根据模板产生字符串, 或者直接打印产生的字符串。以'_'开头的方法都是直接打印产生的字符串
    """
    def __init__(self):
        self.alphalower = list("abcdefghijklmnopqrstuvwxyz")
        self.alphaupper = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        self.digit = list("0123456789")
        self.symbol = list("!@#$%^&*()")
        self.alnum = list("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
                
    def straightline(self, title, length = 100, linestyle = "="):
        """长度为length, 中间文字是title, 线型是linestyle"""
        text = "{:%s^%s}" % (linestyle, length)
        return text.format(title)

    def _straightline(self, title, length = 100, linestyle = "="):
        print(self.straightline(title = title, length = length, linestyle = linestyle))

    def pad_indent(self, text, num_of_indent = 1):
        """在text文字之前, 填充num_of_indent个制表符tab"""
        return "%s%s" % ("\t"*num_of_indent, text)

    def _pad_indent(self, text, num_of_indent = 1):
        print(self.pad_indent(text = text, num_of_indent = num_of_indent))
    
    def randstr(self, length):
        """generate random x-length alpha number string
        """
        s = []
        for _ in range(length): # naive method
            s.append(random.choice(self.alnum))
        return "".join(s)


class Converter():
    """
    [EN]A text format process. Like that captialize first letter, lower other.
    [CN]用于按一定规则处理字符串。
    """
    def strip_formatter(self, text):
        return text.strip()
    
    def person_name_formatter(self, text):
        """将字符串转换为首字母大写, 其他字母小写的, 非严格英文句子格式。单词之间的空格会被标准化为长度1。
        注意: 一些国家, 名字之类的本应大写的单词可能会被转化成小写。
        """
        text = text.strip()
        if len(text) == 0: # 如果是空字符串, 则依旧保留空字符串
            return text
        else:
            text = text.lower()
            # 按照空格拆分单词, 多个空格按一个空格对待
            chunks = [chunk[0].upper() + chunk[1:] for chunk in text.split(" ") if len(chunk)>=1]
            return " ".join(chunks)
        
    def title_formatter(self, text):
        """对字符串进行如下处理
        1. 去掉不必要的空格
        2. 实意单词首字母大写
        """
        text = text.strip()
        if len(text) == 0: # 如果是空字符串, 则依旧保留空字符串
            return text
        else: 
            functional_words = set(["a", "an", "the", "in", "on", "at", "and", "with", "of",
                                    "to", "from", "by"])
            text = text.lower() # 首先去除头尾空格, 并全部小写
            # 按照空格拆分单词, 多个空格按一个空格对待
            chunks = [chunk for chunk in text.split(" ") if len(chunk)>=1]
            
            new_chunks = list()
            for chunk in chunks:
                if chunk not in functional_words:
                    chunk = chunk[0].upper() + chunk[1:]
                new_chunks.append(chunk)
                
            new_chunks[0] = new_chunks[0][0].upper() + new_chunks[0][1:]
            
            return " ".join(new_chunks)
            
    def sentence_formatter(self, text):
        """将字符串转换为首字母大写, 其他字母小写的, 非严格英文句子格式。单词之间的空格会被标准化为长度1。
        注意: 一些国家, 名字之类的本应大写的单词可能会被转化成小写。
        """
        text = text.strip()
        if len(text) == 0: # 如果是空字符串, 则依旧保留空字符串
            return text
        else:
            text = text.lower()
            # 按照空格拆分单词, 多个空格按一个空格对待
            chunks = [chunk for chunk in text.split(" ") if len(chunk)>=1]
            chunks[0] = chunks[0][0].upper() + chunks[0][1:]
            return " ".join(chunks)
    
    def tag_formatter(self, text):
        """对于tag类的字符, 不允许有[",", " ", "\t", "\n"]等这一类的特殊字符, 最标准的tag类字符是只由
        字母, 数字, 下划线构成的字符串
        """
        text = text.strip().lower()
        if len(text) == 0: # 如果是空字符串, 则依旧保留空字符串
            return text
        else:
            # 按照空格拆分单词, 多个空格按一个空格对待
            chunks = [chunk[0].upper() + chunk[1:] for chunk in text.split(" ") if len(chunk)>=1]
            return "_".join(chunks)

class FormatMaster():
    """
    [EN]A abstract class to execute Converter and manipulate Template
    [CN]字符串格式转换器
    """
    def __init__(self):
        self.tpl = Template()
        self.cvt = Converter()
        
    def convert(self, converter, text):
        return converter(text)
    
    def convert_list(self, list_of_text, converter):
        return [converter(text) for text in list_of_text]
    
    def convert_set(self, set_of_text, converter):
        return {converter(text) for text in set_of_text}


fmter = FormatMaster()

if __name__ == "__main__":
    import unittest
    
    class FormatMasterUnittest(unittest.TestCase):
        def test_Template(self):
            self.assertEqual(fmter.tpl.straightline("straight line", 60, "-"),
                "-----------------------straight line------------------------")
            self.assertEqual(fmter.tpl.pad_indent("some message", 2),
                "\t\tsome message")  
            
        def test_Converter(self):
            self.assertEqual(
                fmter.cvt.strip_formatter(" good job "),
                "good job",
                )
            self.assertEqual(
                fmter.cvt.person_name_formatter("   Michael  Jackson"),
                "Michael Jackson",
                )
            self.assertEqual(
                fmter.cvt.title_formatter(" do you want   to build  a snow man? "),
                "Do You Want to Build a Snow Man?",
                )
            self.assertEqual(
                fmter.cvt.sentence_formatter(" do you want   to build  a snow man? "),
                "Do you want to build a snow man?",
                )
            self.assertEqual(
                fmter.cvt.tag_formatter(" korean pop"),
                "Korean_Pop",
                )
            
        def test_FormatMaster(self):
            testdata = [
                        " do you want   to build  a snow man? ",
                        "",
                        "   Michael Jackson",
                        "Boom! "
                        ]
            self.assertListEqual(
                fmter.convert_list(testdata, fmter.cvt.person_name_formatter),
                ["Do You Want To Build A Snow Man?", "", "Michael Jackson", "Boom!"],
                )
            
    unittest.main()