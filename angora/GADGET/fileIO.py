##encoding=UTF8

"""
import:
    from angora.GADGET.fileIO import str2file, file2str
"""

from __future__ import print_function, unicode_literals

def str2file(text, path):
    with open(path, "wb") as f:
        f.write(text.encode("utf8"))
    return

def file2str(path):
    with open(path, "rb") as f:
        return f.read().decode("utf8")
    
if __name__ == "__main__":
    import unittest
    import os

    class fileIOUnittest(unittest.TestCase):
        def test_read(self):
            """The Unicode character U+FEFF is the byte order mark, or BOM, and is used to tell 
            the difference between big- and little-endian UTF-16 encoding. If you decode the web 
            page using the right codec, Python will remove it for you.
            
            ref = http://stackoverflow.com/questions/17912307/u-ufeff-in-python-string
            """
            text1 = file2str(r"test_data\multilanguage.txt")
            text2 = "\r\n".join([r"中\文", r"台/灣", r"グー\グル", r"eng/lish"])
            print([text1])
            print([text2])
        
        def test_write(self):
            text = r"中\文, 台/湾, グー\グル, eng/lish"
            str2file(text, "test.txt")
            self.assertEqual(file2str("test.txt"), text)
        
        def tearDown(self):
            try:
                os.remove("test.txt")
            except:
                pass
            
    unittest.main()