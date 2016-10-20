#! /usr/bin/env python
# encoding:utf-8

"""
@author:hanpy
@time: 16/9/6 14:21
"""
import json
import os
import re
import threading

import time

from spider.httpreq import BasicRequests
from spider.runtime import Log


class ProxyPool(object):
    def auto_load(self): raise NotImplementedError()

    def get_one(self): raise NotImplementedError()

    def get_all(self): raise NotImplementedError()

    def remove_by_key(self, key): raise NotImplementedError()

    def remove_group(self, group): raise NotImplementedError()

    def close(self): raise NotImplementedError()

    def run(self): raise NotImplementedError()

class ADSLProxyPool(ProxyPool, BasicRequests):
    def __init__(self, server="http://120.55.97.254:10000/t/1", user="ipin", password="helloipin"):
        ProxyPool.__init__(self)
        BasicRequests.__init__(self)
        self.proxy_list = list()
        self.proxy_dict = dict()
        self.server = server
        self.user = user
        self.password = password
        self.plock = threading.RLock()
        self.pos = 0
        self.tls = threading.local()
        self.invalid_key = list()
        self.end_flag = False

    def auto_load(self):
        con = self.request_url(url=self.server)
        if not con or con.code != 200:
            raise RuntimeError("访问代理服务器出错,请检查. server=%s " % self.server)
        j = json.loads(con.text)
        if j["code"] != 1:
            raise RuntimeError("服务器返回错误码%d" % j["code"])
        with self.plock:
            while len(self.proxy_list) > 0: self.proxy_list.pop()
            self.proxy_dict.clear()
            for item in j["result"].items():
                key, detail = item[0], item[1]
                # proxy={"http://%s:%s/%s"%(self.user,self.password,detail),
                #        "https://%s:%s/%s"%(self.user,self.password,value)}
                if key not in self.invalid_key:
                    self.proxy_dict[key] = detail
                    self.proxy_list.append(item)

    def default_getter(self):
        with self.plock:
            t = self.pos
            self.pos = (self.pos + 1) % len(self.proxy_list)
            return t % len(self.proxy_list)

    def bind_getter(self):
        with self.plock:
            tid = getattr(self.tls, "tid", -1)
            if tid==-1:
                tid = self.pos
                self.pos = (self.pos + 1) % len(self.proxy_list)
            setattr(self.tls, "tid", tid)
            return tid % len(self.proxy_list)

    def get_one(self, getter="default"):
        if len(self.proxy_list) == 0: return None
        if getter == "bind":
            p = self.bind_getter()
        else:
            p = self.default_getter()
        return self._extract(self.proxy_list[p])

    def _extract(self, r):
        if isinstance(r, tuple) or isinstance(r,list):
            name, info = r
            return {"http": "http://ipin:helloipin@" + info["ip"], "https": "https://ipin:helloipin@" + info["ip"]}

    def get_all(self):
        return self.proxy_dict

    def remove_by_key(self, key):
        self.invalid_key.append(key)
        self.proxy_list = filter(lambda item: item[0] not in self.invalid_key, self.proxy_list)
        for k in self.invalid_key: self.proxy_dict.pop(k)

    def reset_filter(self):
        while len(self.invalid_key) > 0: self.invalid_key.pop()

    def run(self, gap=1):
        self.end_flag=False
        self.loader = threading.Thread(target=self._real_run, args=(gap,))
        self.loader.start()

    def _real_run(self, gap):
        while not self.end_flag:
            self.auto_load()
            time.sleep(gap)

    def close(self):
        self.end_flag = True


class FileProxyPool(ADSLProxyPool):
    def __init__(self, fn):
        ADSLProxyPool.__init__(self)
        self.fn = fn

    def auto_load(self):
        self.end_flag=False
        last_mtime = getattr(self, "last_mtime", 0)
        if last_mtime == os.path.getmtime(self.fn):
            return
        self.last_mtime = os.path.getmtime(self.fn)
        with self.plock:
            while len(self.proxy_list) > 0: self.proxy_list.pop()
            self.proxy_dict.clear()
            i = 0
            with open(self.fn,"r") as f:
                for line in f.xreadlines():
                    if i in self.invalid_key:
                        i+=1
                        continue
                    p = self._match_proxy(line.strip())
                    self.proxy_list.append((i,p))
                    self.proxy_dict[i]=p
                    i+=1
            Log.error("加载%d个代理"%(i))
            return i

    def _extract(self, r):
        if isinstance(r,tuple) or isinstance(r,list):
            return r[1]

    def _match_proxy(self, line):
        m = re.match('([0-9.]+):(\d+):([a-z0-9]+):([a-z0-9._-]+)$', line, re.I)
        m1 = re.match('([0-9.]+):(\d+):([a-z0-9]+)$', line, re.I)
        if m:
            prstr = '%s:%s@%s:%s' % (m.group(3), m.group(4), m.group(1), m.group(2))
            proxies = {'http': 'http://' + prstr, 'https': 'https://' + prstr}
        elif m1:
            prstr = '%s:%s' % (m1.group(1), m1.group(2))
            proxies = {'http': 'http://' + prstr, 'https': 'https://' + prstr}
        else:
            proxies = {'http': 'http://' + line, 'https': 'https://' + line}
        return proxies

class EmptyProxyPool(ProxyPool):
    def remove_group(self, group):
        pass

    def remove_by_key(self, key):
        pass

    def get_one(self):
        return {}

    def run(self):
        pass

    def close(self):
        pass

    def get_all(self):
        return {}

    def auto_load(self):
        pass


if __name__ == "__main__":

    def t_run(id, p):
        while True:
            Log.info(str(id) + " " + p.get_one(getter="bind").__str__())
            time.sleep(1)


    p = FileProxyPool("ipinproxy.txt")
    p.run()

    threads = []
    time.sleep(3)

    for i in range(3):
        t = threading.Thread(target=t_run, args=(i, p,))
        t.start()
        threads.append(t)
    time.sleep(5)
    p.remove_by_key(0)
    time.sleep(10)
    p.reset_filter()
    for t in threads:
        t.join()
