##encoding=utf8

"""
a example project crawling all high school in US from http://high-schools.com
"""

from __future__ import print_function
from bs4 import BeautifulSoup as BS4
import angora.LINEARSPIDER as LS

def level1(url, spider):
    html = spider.html(url)
    soup = BS4(html)
    ul = soup.find("ul", class_ = "list-unstyled state-links")
    for a in ul.find_all("a"):
        url = a["href"]
        info = {"state": a.text}
        yield url, info

def level2(url, spider):
    base_url = "http://high-schools.com"
    html = spider.html(base_url + url)
    soup = BS4(html)
    ul = soup.find("ul", class_ = "cities-ul")
    for a in ul.find_all("a"):
        url = a["href"]
        info = {"city": a.text}
        yield url, info

def level3(url, spider):
    base_url = "http://high-schools.com"
    html = spider.html(base_url + url)
    soup = BS4(html)
    table = soup.find("table", class_ = "table table-striped table-hover table-condensed table-sortable")
    tbody = table.find("tbody")
    
    
    for tr in tbody.find_all("tr"):
        url = tr.td.a["href"]
        info = {key: td.text.strip() for key, td in zip(["school_name", 
                                                         "type", 
                                                         "students", 
                                                         "student_to_teacher_ratio", 
                                                         "free_or_reduced_lunch", 
                                                         "school_distinct"], tr.find_all("td")) }
        yield url, info

def main():
    spider, pm = LS.Crawler(), LS.ProxyManager()
#     pm.download_proxy(100)
#     pm.load_pxy()
#     pm.reset_health()
#     spider.enable_proxy(pm)

    entrance_url = "http://high-schools.com/"
    sch = LS.Scheduler(entrance_url)
    sch.bind(spider=spider, try_howmany=10, save_interval=20, local_file="task.p")
    sch.bind_link_extractor([level1, level2, level3])
    sch.start()
    print("====== COMPLETE ======")

# main()

def exam():
    """You can check your crawling progress by using this function
    """
    from angora.DATA.pk import load_pk
    from angora.DATA.js import dump_js
    from angora.DATA.dicttree import DictTree
    task = load_pk("task.p")
    dump_js(task, "task.json", replace = True)
    DictTree.stats_on_level(task, 0)
    DictTree.stats_on_level(task, 1)
    DictTree.stats_on_level(task, 2)
    DictTree.stats_on_level(task, 3)
    
exam()