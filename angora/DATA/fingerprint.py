##encoding=utf-8

"""
Copyright (c) 2015 by Sanhe Hu
------------------------------
    Author: Sanhe Hu
    Email: husanhe@gmail.com
    Lisence: LGPL
    

Module description
------------------
    This module is built on python standard lib - hashlib. We minimized the syntax to 
    calculate a hash value of a string, a python object or a file.


Keyword
-------
    hash


Compatibility
-------------
    Python2: Yes
    Python3: Yes
    
    
Prerequisites
-------------
    None


Import Command
--------------
            
"""


from __future__ import print_function
import hashlib
import pickle
import sys

is_py2 = (sys.version_info[0] == 2)
if is_py2:
    pickle_protocol = 2
else:
    pickle_protocol = 3
    
class FingerPrint():
    """A hashlib wrapper class allow you to use one command to do any hash using 
    specified algorithm
    """
    def __init__(self):
        self.default_hash_method = hashlib.md5
    
    def use(self, algorithm):
        """change the hash algorithm you gonna use
        """
        algorithm = algorithm.lower()
        if algorithm == "md5":
            self.default_hash_method = hashlib.md5
        elif algorithm == "sha1":
            self.default_hash_method = hashlib.sha1
        elif algorithm == "sha224":
            self.default_hash_method = hashlib.sha224
        elif algorithm == "sha256":
            self.default_hash_method = hashlib.sha256
        elif algorithm == "sha384":
            self.default_hash_method = hashlib.sha384
        elif algorithm == "sha512":
            self.default_hash_method = hashlib.sha512
        else:
            raise Exception("There's no algorithm names '%s'!" % algorithm)
        
    def of_text(self, text, encoding="utf-8"):
        """use default hash method to return hash value of a piece of string
        default setting use 'utf-8' encoding.
        """
        m = self.default_hash_method()
        m.update(text.encode(encoding))
        return m.hexdigest()
    
    def of_pyobj(self, pyobj):
        """use default hash method to return hash value of a piece of Python picklable object
        """
        m = self.default_hash_method()
        m.update(pickle.dumps(pyobj, protocol=pickle_protocol))
        return m.hexdigest()
    
    def of_file(self, abspath, chunk_size=2**10):
        """use default hash method to return hash value of a piece of a file
        Estimate processing time on:
            CPU = i7-4600U 2.10GHz - 2.70GHz, RAM = 8.00 GB
            1 second can process 0.25GB data
                0.59G - 2.43 sec
                1.3G - 5.68 sec
                1.9G - 7.72 sec
                2.5G - 10.32 sec
                3.9G - 16.0 sec
        
        ATTENTION:
            if you change the meta data (for example, the title, years information
            in audio, video) of a multi-media file, then the hash value gonna also change.
        """
        m = self.default_hash_method()
        with open(abspath, "rb") as f:
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                m.update(data)
        return m.hexdigest()
    
fingerprint = FingerPrint()
    
if __name__ == "__main__":
    import unittest
    
    class FingerPrintUnittest(unittest.TestCase):
        def test_md5(self):
            for algorithm in ["md5", "sha1", "sha224", "sha256", "sha384", "sha512"]:
                print("===%s hash value===" % algorithm)
                fingerprint.use(algorithm)
                print(fingerprint.of_text("message"))
                print(fingerprint.of_pyobj({"key": "value"}))
                print(fingerprint.of_file("fingerprint.py"))

    unittest.main()