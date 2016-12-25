#! /usr/bin/env python
# encoding:utf-8

"""
@author:chentao
@time: 2016/12/15 16:17
"""

import copy
import json
import os
import random
import re
import time

from spider.httpreq import SessionRequests

def utf8str(obj):
    if isinstance(obj, unicode):
        return obj.encode('utf-8')
    if isinstance(obj, str):
        return obj

    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, unicode):
                obj[key] = value.encode('utf-8')

    if isinstance(obj, dict) or isinstance(obj, list):
        return utf8str(json.dumps(obj, ensure_ascii=False, sort_keys=True))
    return utf8str(str(obj))

class DaZhongDianPing():
    def __init__(self):
        self.req = SessionRequests()
        self.req.select_user_agent("=Mozilla/5.0 (Windows NT 10.0; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0")
        self.while_request("http://www.dianping.com/guangzhou")
        self.lst_category = []

    def init(self):
        text = self.while_request("http://www.dianping.com/search/category/4/10")
        lst = re.findall('href="(/search/category/4/10/g\d+)"', text, re.I)
        i = 0
        for l in lst:
            url = "http://www.dianping.com" + l
            self.lst_category.append(url)
            i += 1
            print i, url


    def while_request(self, url, **kwargs):
        retry = 0
        while retry < 10:
            con = self.req.request_url(url, **kwargs)
            if con is None or con.code != 200:
                print "con is None " if con is None else "con.code = %d " % con.code, "retry: %d " % retry
                time.sleep(1)
                continue
            else:
                return con.text
        return None

    def test(self ,url):
        con = self.req.request_url(url)
        print con.text

if __name__ == "__main__":
    h = DaZhongDianPing()
    h.init()
    #h.test("http://www.dianping.com/search/category/4/10")