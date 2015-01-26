##encoding=utf8

"""Http Crawler
Repack of requests

compatibility: compatible to python2 and python3

prerequisites: requests
    ProxyManager need pandas

import:
    from angora.LINEARSPIDER.crawler import Crawler, ProxyManager
"""

from __future__ import print_function
from bs4 import BeautifulSoup as BS4
try: # if doens't have pandas, ProxyManager will not work
    import pandas as pd
except:
    pass    
import requests
import random
import sys

is_py2 = (sys.version_info[0] == 2)
if is_py2:
    reload(sys); # change the system default encoding = utf-8
    eval("sys.setdefaultencoding('utf-8')")

class Crawler(object):
    """Advanced http crawler class
    """
    def __init__(self):
        self.user_agents = ["Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36", #Chrome 41.0.2228.0
                            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36", #Chrome 37.0.2062.124
                            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.62 Safari/537.36", #Chrome 29.0.1547.62
                            
                            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10; rv:33.0) Gecko/20100101 Firefox/33.0", #Firefox 33.0
                            "Mozilla/5.0 (X11; Linux i586; rv:31.0) Gecko/20100101 Firefox/31.0", #Firefox 31.0
                            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20120101 Firefox/29.0", #Firefox 29.0
                            
                            "Mozilla/5.0 (X11; Linux x86_64; rv:17.0) Gecko/20121202 Firefox/17.0 Iceweasel/17.0.1", #Iceweasel 17.0.1
                            "Mozilla/5.0 (X11; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1 Iceweasel/15.0.1", #Iceweasel 15.0.1
                            "Mozilla/5.0 (X11; debian; Linux x86_64; rv:15.0) Gecko/20100101 Iceweasel/15.0", #Iceweasel 15.0
                            
                            "Mozilla/5.0 (compatible, MSIE 11, Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko", #Internet Explorer 11.0
                            "Mozilla/5.0 (compatible; MSIE 10.6; Windows NT 6.1; Trident/5.0; InfoPath.2; SLCC1; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 2.0.50727) 3gpp-gba UNTRUSTED/1.0", #Internet Explorer 10.6
                            "Mozilla/5.0 (compatible; MSIE 10.0; Macintosh; Intel Mac OS X 10_7_3; Trident/6.0)", #Internet Explorer 10.0
                            "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)", #Internet Explorer 9.0
                            
                            "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/533.1 (KHTML, like Gecko) Maxthon/3.0.8.2 Safari/533.1", #Maxthon 3.0.8.2
                            "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.4 (KHTML, like Gecko) Maxthon/3.0.6.27 Safari/532.4", #Maxthon 3.0.6.27
                            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; Maxthon/3.0)", #Maxthon 3.0
                            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/4.0; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; Zune 4.0; InfoPath.3; MS-RTC LM 8; .NET4.0C; .NET4.0E; Maxthon 2.0)", #Maxthon 2.0
                            
                            "Mozilla/5.0 (Windows; U; Win 9x 4.90; SG; rv:1.9.2.4) Gecko/20101104 Netscape/9.1.0285", #Netscape 9.1.0285
                            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X; en-US; rv:1.8.1.8pre) Gecko/20071001 Firefox/2.0.0.7 Navigator/9.0RC1", #Netscape 9.0RC1
                            "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.1.12) Gecko/20080219 Firefox/2.0.0.12 Navigator/9.0.0.6", #Netscape 9.0.0.6
                            ]
        
        self.default_header = {"Accept":"text/html;q=0.9,*/*;q=0.8",
                               "Accept-Charset":"ISO-8859-1,utf-8;q=0.7,*;q=0.3",
                               "Accept-Encoding":"gzip",
                               "Connection":"close",
                               "Referer": None}
        self.auth = None # initialize the self.auth. Then if there"s no login, Crawler.html can work in regular mode
        self.using_proxy = 0
    
    def __str__(self):
        textbox = list()
        if self.using_proxy:
            textbox.append("using following proxies:\n\t%s\n" % self.pm)
            textbox.append("currently using proxy:\n\t%s\n" % self.pm.current_proxy)
        textbox.append("default_header:\n\t%s\n" % self.default_header)
        return "\n".join(textbox)
        
    def set_referer(self, url):
        """set a referer link. This is an Anti "anti-leech" technique
        usually set the referer link to the website you are crawling.
        """
        self.default_header["Referer"] = url
    
    def enable_proxy(self, proxymanager):
        """Activate proxy"""
        self.pm = proxymanager
        self.using_proxy = 1
    
    def disable_proxy(self):
        """disable proxy"""
        self.using_proxy = 0
        
    def _gen_header(self):
        """generate a random header
        """
        self.default_header["User-Agent"] = random.choice(self.user_agents)
        return self.default_header
    
    def login(self, url, payload, timeout = 6):
        """website log in
            url = login_page_url
            payload = {key1: acc, key2: password}
        """
        self.auth = requests.Session()
        try:
            self.auth.post(url, data=payload, timeout=timeout)
            print("successfully logged in to %s" % url)
            return True
        except:
            return False
    
    def html_WITH_proxy(self, url, timeout = 6, lock_proxy = False, enable_verbose = False):
        """return the html of the url
        [Argv]
        ------
        url: the url you want to crawl
        
        timeout: http request time out setting, if time out, return None
        
        lock_proxy: boolean
            if True, then use last proxy instead of new proxy generated by proxy manager
            
        enable_verbose: boolean
            if True, then print the current proxy
        
        Notice:
            ressponse.text always returns bytes (in python2, it calls str; in python3 it calls bytes)
        THUS:
            always encoding the bytes into utf-8 is a good choice
        """
        if lock_proxy:
            proxies = {"http": self.pm.current_proxy}
        else:
            proxies = self.pm.generate_one()
        
        if enable_verbose:
            print("\tCurrently using: %s" % proxies)
            
        if not self.auth: # if no login needed, then self.auth = None, then not self.auth = True
            ## regular get html, use requests.get
            try:
                response = requests.get(url, 
                                        headers = self._gen_header(), 
                                        timeout = timeout,
                                        proxies = proxies)
                return response.text.encode("utf-8") # if success, return html
            except:
                self.pm.update_health(0) # failed, then update proxy health with FAILED
                return None # if failed, return none
        else: # if login needed, use self.auth.get
            try:
                response = self.auth.get(url, 
                                         headers = self._gen_header(), 
                                         timeout = timeout,
                                         proxies = proxies)
                return response.text.encode("utf-8") # if success, return html
            except:
                self.pm.update_health(0) # failed, then update proxy health with FAILED
                return None # if failed, return none

    def html_WITHOUT_proxy(self, url, timeout = 6):
        """return the html of the url
        Notice:
            ressponse.text always returns bytes (in python2, it calls str; in python3 it calls bytes)
        THUS:
            always encoding the bytes into utf-8 is a good choice
        """
        if not self.auth: # if no login needed, then self.auth = None, then not self.auth = True
            ## regular get html, use requests.get
            try:
                response = requests.get(url, headers = self._gen_header(), timeout = timeout)
                return response.text.encode("utf-8") # if success, return html
            except:
                return None # if failed, return none
        else: # if login needed, use self.auth.get
            try:
                response = self.auth.get(url, headers = self._gen_header(), timeout = timeout)
                return response.text.encode("utf-8") # if success, return html
            except:
                return None # if failed, return none
    
    def html(self, url, timeout = 6, lock_proxy = False, enable_verbose = False):
        """universal method for self.html_with_proxy or self.html_without_proxy"""
        if self.using_proxy:
            return self.html_WITH_proxy(url, timeout, lock_proxy, enable_verbose)
        else:
            return self.html_WITHOUT_proxy(url, timeout)
        
    def save_html(self, url, save_as, timeout = 10):
        """save the html to save_as, which is a .html local file
        """
        html = self.html(url)
        if html:
            with open(save_as, "wb") as f:
                f.write(html)
            
    def download(self, url, save_as, timeout = 10):
        """download the file by url to the path of save_as
        """
        if not self.auth:
            try:
                response = requests.get(url, headers = self._gen_header(), timeout = timeout, stream=True)
                with open(save_as, "wb") as f:
                    for block in response.iter_content(1024):
                        if not block:
                            break
                        f.write(block)
            except:
                pass
        else:
            try:
                response = self.auth.get(url, headers = self._gen_header(), timeout = timeout, stream=True)
                with open(save_as, "wb") as f:
                    for block in response.iter_content(1024): # use buffer to download
                        if not block:
                            break
                        f.write(block)
            except:
                pass

class ProxyManager(object):
    """
    [EN]
    [CN]代理管理器
    在爬虫程序中，为了不因为短时间内对服务器太多次的访问而导致被block，所以采用代理技术隐瞒自己的IP是
    重要技巧。假如我们在网上找到了10个代理，但并不是每个代理都够稳定能用，所以我们需要采用计算代理健康
    度的方法来评估哪个代理能用，哪个代理不能用。
    
    健康度评估算法：
        每个代理有三个属性: 成功次数，尝试次数，健康度
        每次使用代理，如果成功，则成功次数和尝试次数+1
        每次使用代理，如果失败，则成功次数和尝试次数+1
        健康度在尝试次数小于5次时都是1.0，如果尝试次数多于5次，则健康度 = 成功次数/尝试次数
    
    代理管理器主要有如下几个功能：
        1. 从健康度较高的代理中随机选取一个代理使用。（高于0.75）
        2. 根据上一次代理使用的
    """
    def __init__(self):
        self.current_proxy = None
        self.file_path = "proxy.txt"
        
    def __str__(self):
        try:
            return str(self.proxy)
        except:
            return "No available Proxy"
        
    def download_proxy(self, maximum_num_of_proxy = 10):
        """
        [EN]load latest availble proxy from www.us-proxy.org
        There are 3 levels of proxies according to their anonymity.
        Level 1 - Elite Proxy / Highly Anonymous Proxy: The web server can't detect whether you are using a proxy.
        Level 2 - Anonymous Proxy: The web server can know you are using a proxy, but it can't know your real IP.
        Level 3 - Transparent Proxy: The web server can know you are using a proxy and it can also know your real IP.
        
        [CN]从www.us-proxy.org上抓取我们需要的代理
        在=== EDIT THE FOLLOWING RULES CAN FILTER THE PROXY YOU WANT 一行下可以修改规则，确定你所需要的
        代理。默认只使用Elite proxy
        """
        ### get www.us-proxy.org homepage html
        spider = Crawler()
        html = spider.html("http://www.us-proxy.org/")
        
        ### analyze the html, save useful proxy.
        ips = list()
        res = list()
        
        soup = BS4(html)
        table = soup.find("table", id = "proxylisttable")
        for tr in table.tbody.find_all("tr"):
            ip, port, code, country, anonymity, google, https, last_check = [td.text for td in tr.find_all("td")]
            ### === EDIT THE FOLLOWING RULES CAN FILTER THE PROXY YOU WANT 
            if anonymity == "elite proxy": # default only use elite proxy
                ips.append("http://%s:%s" % (ip, port))
                res.append([0.0, 0.0, 1.0])
                if len(res) >= maximum_num_of_proxy: # if got enough useful proxy, then step out
                    break
        
        self.proxy = pd.DataFrame(res, index = ips, columns = ["success", "tried", "health"])
        
    def dump_pxy(self):
        """dump currently using proxy data to local file in descent order by health"""
        self.proxy.sort("health", ascending=0).to_csv(self.file_path, sep="\t", header = True, index = True)
    
    def load_pxy(self, replace = False):
        """load proxy data from local file and merge with current using proxy"""
        df = pd.read_csv(self.file_path, sep="\t", header = 0, index_col = 0)
        if replace: # if replace = True, delete current proxy before loading
            self.proxy = df
        else: # if replace = False, merge current proxy with loaded proxy from self.file_path
            try:
                for row_ind, row in df.iterrows():
                    self.proxy.loc[row_ind, :] = row
            except:
                self.proxy = df
                
    def reset_health(self):
        """reset currently using proxy ip to fresh new
        success = 0.0, tried = 0.0, health = 1.0
        """
        self.proxy[["success", "tried"]] = 0.0
        self.proxy["health"] = 1.0
    
    def generate_one(self):
        """randomly choose a proxy with health greater than 0.75
        """
        health_proxy = self.proxy[self.proxy["health"] >= 0.75].index
        self.current_proxy = random.choice(health_proxy)
        return {"http": self.current_proxy}

    def update_health(self, successed):
        """update proxy health after you using proxy to visit any url
        successed: boolean, the http request using the proxy succeed or not
        """
        if self.current_proxy:
            ip = self.current_proxy
        else:
            raise Exception("ERROR: never generate any proxy; please use self.generateone method first")
        
        if successed: # if is successful, tried + 1, success + 1
            self.proxy.loc[ip, ["success", "tried"]] += 1.0
            if self.proxy.loc[ip, "tried"] >= 5: # if tried more than 10 times, then we keep update successful rate
                self.proxy.loc[ip, "health"] = float(self.proxy.loc[ip, "success"])/self.proxy.loc[ip, "tried"]
        else: # if failed, tried + 1, success remain
            self.proxy.loc[ip, "tried"] += 1.0
            if self.proxy.loc[ip, "tried"] >= 5: # if tried more than 10 times, then we keep update successful rate
                self.proxy.loc[ip, "health"] = float(self.proxy.loc[ip, "success"])/self.proxy.loc[ip, "tried"]

if __name__ == "__main__":
    class ProxyManager_unittest():
        @staticmethod
        def download_proxy():
            pm = ProxyManager()
            print(pm)
            pm.download_proxy() # see if successfully get latest available proxy from www.us-proxy.org
            print(pm)
        
        @staticmethod
        def dump_pxy():
            print("{:=^100}".format("dump_pxy"))
            pm = ProxyManager()
            pm.download_proxy()
            pm.dump_pxy()
            
        @staticmethod
        def load_pxy():
            print("{:=^100}".format("load_pxy"))
            pm = ProxyManager()
            print("{:=^60}".format("before"))
            print(pm)
            pm.load_pxy()
            print("{:=^60}".format("after"))
            print(pm)
            
        @staticmethod
        def reset_health():
            print("{:=^100}".format("load_pxy"))
            pm = ProxyManager()
            pm.load_pxy()
            print("{:=^60}".format("before"))
            print(pm)
            pm.reset_health()
            print("{:=^60}".format("after"))
            print(pm)
            
        @staticmethod
        def generate_one():
            pm = ProxyManager()
            pm.download_proxy()
            for _ in range(10):
                print(pm.generate_one()) # sample proxies = {"http": "http://10.10.1.10:3128", "https": "http://10.10.1.10:1080",}
                print("\tcurrently using:", pm.current_proxy) # see if proxy manager saved the most recent proxy to self.current_proxy
        
        @staticmethod
        def update_health():
            pm = ProxyManager()
            pm._equip_proxy()
            try:
                pm.update_health(1)
            except Exception as e:
                print(e)
                
            print(pm.generate_one())
            pm.update_health(1)
            print(pm)


    class Crawler_unittest():
        @staticmethod
        def set_referer():
            print("{:=^100}".format("set_referer"))
            spider = Crawler()
            print(spider)
            spider.set_referer("https://www.python.org/")
            print(spider)
            
        @staticmethod
        def enable_proxy():
            print("{:=^100}".format("enable_proxy"))
            spider = Crawler()
            pm = ProxyManager()
            pm.download_proxy()
            spider.enable_proxy(pm)
            print(spider)
            
        @staticmethod
        def html_WITHOUt_proxy():
            """test normal http request"""
            url = "http://docs.python-requests.org/"
            spider = Crawler()
            html = spider.html(url)
            print(BS4(html).prettify())
            
        @staticmethod
        def html_WITH_proxy():
            """test random proxy mechanism"""
            url = "http://docs.python-requests.org/"
            spider = Crawler()
            pm = ProxyManager()
            pm.download_proxy()
            spider.enable_proxy(pm)
            for i in range(10):
                html = spider.html(url)
                print(i, spider.pm.current_proxy)
                if html:
                    print("\tSUCCESS")
                    spider.pm.update_health(1)
                else:
                    print("\tFAILED")
            print(spider)

#     ProxyManager_unittest.download_proxy()
#     ProxyManager_unittest.dump_pxy()
#     ProxyManager_unittest.load_pxy()
#     ProxyManager_unittest.clear_health()
#     ProxyManager_unittest.generate_one()
#     ProxyManager_unittest.update_health()

#     Crawler_unittest.set_referer()
#     Crawler_unittest.enable_proxy()
#     Crawler_unittest.html_WITHOUt_proxy()
#     Crawler_unittest.html_WITH_proxy()