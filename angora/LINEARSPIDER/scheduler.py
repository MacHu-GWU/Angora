##encoding=utf8

"""
A Linear Link Extractor Scheduler automatically handle:

    1. link extract from level1, level2,...
    2. Auto detect with url has been crawled and never do repetitive job.
    
"""

from __future__ import print_function
from angora.DATA import *
from angora.DATA.dicttree import DictTree as DT
from angora.GADGET import *
from .crawler import Crawler
import itertools

class Scheduler(object):
    def __init__(self, entrance_url, help = False):
        if help:
            helpinfo = \
            """
            ToDo...
            """
            print(helpinfo)
        self.entrance_url = entrance_url
        self.log = Log()
        
    def bind_default(self):
        
        self.spider = Crawler()
        self.try_howmany = 1
        self.save_interval = 10
        self.local_file = "default_schelduler.json"
        
    def bind(self, spider, try_howmany, save_interval, local_file):
        """bind the scheduler with the following:
        spider: the spider you want to use.
            before you bind it, make sure
        """
        self.spider = spider
        self.try_howmany = try_howmany
        self.save_interval = save_interval
        self.local_file = local_file
        
    def bind_link_extractor(self, link_extractor_list):
        """
        [CN]link extractor 函数模板
            link extractor 函数用于从网页上抽取出该网页上用户所需要的所有子 url 链接，以及子 url 的
            metadata。link extractor实际是一个生成器函数。以当前url和所使用的LinearSpider.crawler.Crawler
            为输入，生成所有的子url和子url的metadata。所以 link extractor函数的输入输出定义如下：
            
            [Args]
            ------
            url: the url you are trying to extract sub url from
            
            spider: the LinearSpider.crawler.Crawler instance you are using
            
            [Returns]
            ---------
            url: one sub url you need
            
            metadata: a dictionary like {"#attribute_name" : #attribute_value} 
            
            [example link extractor function]
            def level1(url, spider):
                base_url = "http://high-schools.com" # extract sub url from this url
                html = spider.html(url) # get html
                soup = BS4(html) # parse it as beautifulsoup
                ul = soup.find("ul", class_ = "list-unstyled state-links") # find sub url you want
                for a in ul.find_all("a"):
                    url = base_url + a["href"] # create sub url
                    info = {"state": a.text} # create metadata dictionary
                    yield url, info # yield
        """
        self.link_extractor_list = link_extractor_list

    def _update_node(self, key, node, link_extractor):
        for url, info in link_extractor(key, self.spider):
            DT.add_children(node, url, **info)

    def start(self):
        """
        [CN]线型爬虫的link extractor满足原子性。即要么该页面下所有的子页面都被成功摘取出来，要么都不成功。
        这样就能保证一个父url下要么没有子url，要么成功的摘取了页面上所有的子url。所以我们只要检查父节点的
        子节点数，只有在等于0的时候，我们才发送http请求，尝试解析网页
        """
        try:
            self.sitemap = load_pk(self.local_file)
        except:
            self.sitemap = {"!!": {"key": self.entrance_url}}
        
        cycler = itertools.cycle(range(self.save_interval))
        for level, link_extractor in zip(list(range(len(self.link_extractor_list))), 
                                         self.link_extractor_list):
            for key, node in DT.kv_level(self.sitemap, level):
                if DT.length(node) == 0: # 只有在等于0的时候才说明没爬过
                    print("crawling %s" % key)
                    try:
                        tryit(self.try_howmany, self._update_node, key, node, link_extractor) # 尝试爬若干次
                        if self.spider.using_proxy:
                            self.spider.pm.update_health(1) # 如果成功，更新代理的健康度
                        # 检查循环计数器，说明已经成功解析过了#save_interval个url
                        # 则需要将文件dump到本地进行保存
                        if next(cycler) == (self.save_interval - 1):
                            safe_dump_pk(self.sitemap, self.local_file)
                            if self.spider.using_proxy:
                                self.spider.pm.dump_pxy()
                    except Exception as e:
                        if self.spider.using_proxy:
                            self.log.write(e, "key=%s, proxy=%s" % (key, self.spider.pm.current_proxy))
                        else:
                            self.log.write(e, "key=%s" % (key,))
        ## 结束了
        safe_dump_pk(self.sitemap, self.local_file)
        if self.spider.using_proxy:
            self.spider.pm.dump_pxy()
            
