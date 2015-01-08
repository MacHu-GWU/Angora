##encoding=utf8

"""
import:
    from angora.STRING.formatmaster import FormatMaster
"""

from __future__ import print_function

class Converter():
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
        text = text.strip()
        if len(text) == 0: # 如果是空字符串, 则依旧保留空字符串
            return text
        else:
            # 按照空格拆分单词, 多个空格按一个空格对待
            chunks = [chunk for chunk in text.split(" ") if len(chunk)>=1]
            return "_".join(chunks)
    
class FormatMaster():
    """字符串格式转换器
    """
    def __init__(self):
        self.converter = Converter()
    
    def convert(self, converter, text):
        return converter(text)
    
    def convert_list(self, converter, list_of_text):
        return [converter(text) for text in list_of_text]
    
    def convert_set(self, converter, set_of_text):
        return {converter(text) for text in set_of_text}

if __name__ == "__main__":
    def unittest_FormatMaster():
        fm = FormatMaster()
        
        testdata = [
                    " do you want   to build  a snow man? ",
                    "",
                    "   Michael Jackson",
                    "Boom! "
                    ]
    
        print("{:=^100}".format("person_name_formatter"))
        for text in testdata:
            print("[%s]=>[%s]" %(text, fm.convert(fm.converter.person_name_formatter, text) ) )
    
        print("{:=^100}".format("sentence_formatter"))
        for text in testdata:
            print("[%s]=>[%s]" %(text, fm.convert(fm.converter.sentence_formatter, text) ) )
        
        print("{:=^100}".format("title_formatter"))
        for text in testdata:
            print("[%s]=>[%s]" %(text, fm.convert(fm.converter.title_formatter, text) ) )
            
        print("{:=^100}".format("tag_formatter"))
        for text in testdata:
            print("[%s]=>[%s]" %(text, fm.convert(fm.converter.tag_formatter, text) ) )
        
        print("{:=^100}".format("batch_process"))
        print(fm.convert_list(fm.converter.person_name_formatter, testdata))
        
    unittest_FormatMaster()