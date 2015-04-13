##encoding=utf-8

"""
Copyright (c) 2015 by Sanhe Hu
------------------------------
    Author: Sanhe Hu
    Email: husanhe@gmail.com
    Lisence: LGPL
    

Module description
------------------
    simplecrawler is a module to do http request smartly.
    
    Features:
        1. Automatically handle html encoding problem. You can also name the encoding.
        2. You can login website by calling .login(url, payload) method.
        3. Easily set timeout.
        4. Easily set sleeptime, the crawler sleep a while before sending every http requests.
        5. Easily save html text to a local file.
        6. Easily download binary file at url.
        
Keyword
-------
    crawler, tool
    
Compatibility
-------------
    Python2: Yes
    Python3: Yes
    
Prerequisites
-------------
    requests > 2.3.0, https://pypi.python.org/pypi/requests
    chardet > 2.2.1, https://pypi.python.org/pypi/chardet

Import Command
--------------
    from angora.LINEARSPIDER.simplecrawler import SmartDecoder, SimpleCrawler
    
"""

from __future__ import print_function
import requests
import time
try:
    import chardet
except:
    pass

class SmartDecoder():
    """A stable bytes decoder.
    """
    def catch_position_in_UnicodeDecodeError_message(self, text):
        for token in text.replace(":", "").split(" "):
            try:
                return int(token)
            except:
                pass
        raise Exception("unable to find position from '%s'" % text)
    
    def decode(self, a_bytes, encoding):
        """
        Extremed decoding feature
        -------------------------
            Some time most of byte are encoded correctly. So chardet is able to detect the encoding.
            But, sometime some bytes in the middle are notn encoded correctly. So it is still unable
            to apply bytes.decode("encoding-method")
            
            e.g.::
                b"82347912350898143059043958290345" 3059 is not right.
                  [-----Good----][-Bad][---Good---]

            What we do is to drop those bad encoded bytes, and try to recovery text as much as
            possible. So this method is to recursively call it self, and trying to decode good
            bytes chunk, and finally concatenate them together
            
        [args]
        ------
            a_bytes: the bytes that encoding is unknown
            encoding: how you gonna decode a_bytes
            
        [return]
        --------
            example returns: (text, encoding)
                text: a text that a_bytes.decode(encoding)
                encoding: the name of encoding
        """
        try:
            return (a_bytes.decode(encoding), encoding)
        except Exception as e:
            ind = self.catch_position_in_UnicodeDecodeError_message(str(e))
            return (a_bytes[:ind].decode(encoding) + self.decode(a_bytes[(ind+2):], encoding)[0],
                    encoding)
            
    def autodecode(self, a_bytes):
        """Automatically detect encoding, and decode bytes
        """
        try: # 如果装了chardet
            analysis = chardet.detect(a_bytes)
            if analysis["confidence"] >= 0.75: # 如果可信
                return (self.decode(a_bytes, analysis["encoding"])[0], 
                        analysis["encoding"])
            else: # 如果不可信, 打印异常
                raise Exception("Failed to detect encoding. (%s, %s)" % (
                                            analysis["confidence"],
                                            analysis["encoding"]))
        except NameError: # 如果没有装chardet
            print("Warning! chardet not found. Use utf-8 as default encoding instead.")
            return (a_bytes.decode("utf-8")[0], 
                    "utf-8")
            

class SimpleCrawler():
    def __init__(self, timeout=6, sleeptime=0):
        self.auth = requests
        self.default_timeout = timeout
        self.default_sleeptime = sleeptime
        self.default_header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.118 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, sdch",
            "Accept-Language": "en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4",
            "Content-Type": "text/html; charset=UTF-8",
            "Connection": "close",
            "Referer": None,
            }
    
        self.decoder = SmartDecoder()
        self.domain_encoding_map = dict()
        
    def set_timeout(self, timeout):
        """set default timeout limit in second
        """
        self.default_timeout = timeout
    
    def set_sleeptime(self, sleeptime):
        """change default_sleeptime
        """
        self.default_sleeptime = sleeptime

    def set_referer(self, url):
        """set a referer link. This is an Anti "anti-leech" technique
        usually set the referer link to the website you are crawling.
        """
        self.default_header["Referer"] = url
    
    def login(self, url, payload):
        """website log in
            url = login_page_url
            payload = {key1: acc, key2: password}
        """
        self.auth = requests.Session()
        try:
            self.auth.post(url, data=payload, timeout=self.default_timeout)
            print("successfully logged in to %s" % url)
            return True
        except:
            return False

    def get_domain(self, url):
        """return the domain of this url
        """
        return "/".join(url.split("/")[:3])

    def get_response(self, url, timeout=None):
        """return http request response
        """
        if not timeout:
            timeout = self.default_timeout
            
        if self.default_sleeptime:
            time.sleep(self.default_sleeptime)
            
        try:
            return self.auth.get(url, headers=self.default_header, timeout=self.default_timeout)
        except:
            return None
    
    def html(self, url, timeout=None):
        """high level method to get http request response in text.
        smartly handle the encoding problem.
        """
        response = self.get_response(url, timeout=timeout)
        if response:
            domain = self.get_domain(url)
            if domain in self.domain_encoding_map: # domain have been visited
                try: # apply extreme decoding
                    html = self.decoder.decode(response.content, 
                                               self.domain_encoding_map[domain])[0]
                    return html
                except:
                    return None
            else: # never visit this domain 
                try:
                    html, encoding = self.decoder.autodecode(response.content)
                    self.domain_encoding_map[domain] = encoding # save chardet analysis result
                    return html
                except:
                    return None
        else:
            return None
            
    def download(self, url, dst, timeout=None):
        """download the binary file at url to distination path
        """
        response = self.get_response(url, timeout=timeout)
        if response:
            with open(dst, "wb") as f:
                for block in response.iter_content(1024):
                    if not block:
                        break
                    f.write(block)
    
if __name__ == "__main__":
    spider = SimpleCrawler()
    
    url = "http://www.caribbeancom.com/moviepages/010103-304/index.html"
    html = spider.html(url)
    print(type(html))
    
    url = "http://www.ralphlauren.fr/home/index.jsp?locale=it_FR&ab=global_cs_italy_US"
    html = spider.html(url)
    print(type(html))
    
    url = "https://pypi.python.org/pypi/requests/2.6.0"
    html = spider.html(url)
    print(type(html))

    url = "https://www.python.org/doc/"
    html = spider.html(url)
    print(type(html))
    
    print(spider.domain_encoding_map)