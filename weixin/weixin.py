#! /usr/bin/env python
# encoding:utf-8

"""
@author:hanpy
@time: 2016/10/18 18:11
"""
import json
import os
import random
import re

import time

import pymongo

from spider.CommonSpider import CommonSpider, Dispatcher, Saver
from spider.MQManager import RawQManager
from spider.ProxyPool import FileProxyPool, EmptyProxyPool
from spider.httpreq import SessionRequests, BasicRequests
from spider.runtime import Log
from spider.savebin import FileSaver


class SogouWeiXinSpider(CommonSpider):

    class Dispatcher(Dispatcher):
        def run(self):
            if not os.path.exists("key_list.txt"):
                print "can not find key_list.txt."
                return
            with open("key_list.txt", "r") as f:
                key_list = f.read().split(",")
                for key in key_list:
                    if not key:continue
                    job={"key":key, "type":"u1"}
                    self.queue_manager.put_main_job(job)

    class Saver(Saver):
        def __init__(self):
            Saver.__init__(self)
            self.fail_saver = FileSaver("failed.log")
            self.succ_saver = FileSaver("succ.log")
            self.mongo_client = pymongo.MongoClient()
            self.article_list_table = self.mongo_client["weixin_article"]["article_list"]
            self.article_detail_table = self.mongo_client["weixin_article"]["article_detail"]

        def fail_save(self, job, **kwargs):
            self.fail_saver.append(json.dumps(job))

        def succ_save(self, res, **kwargs):
            # self.succ_saver.append(json.dumps(res))
            self.article_list_table.insert_one(res)

        def save_article_list(self, data):
            self.article_list_table.insert_one(data)

        def save_article_detail(self, data, url):
            data['url']=url
            data['date']=time.strftime("%Y-%m-%d")
            self.article_detail_table.insert_one(document=data)

        def close(self):
            self.mongo_client.close()

    def __init__(self, worker_count):
        CommonSpider.__init__(self, spider_name="sogou_wexin", worker_count=worker_count)
        self.queue_manager = RawQManager()
        self.dispatcher = self.Dispatcher(self.queue_manager)
        # self.proxy_pool = FileProxyPool()
        self.proxy_pool = EmptyProxyPool()
        self.saver = self.Saver()

    def rebuild_req(self, sreq, conn, headers):
        time.sleep(1)
        ABTEST = sreq.get_cookie("ABTEST")
        SUID = sreq.get_cookie("SUID")
        p = int(ABTEST.split("|")[0])
        random_int = random.randrange(p + 1, len(SUID))
        params = {}
        mt = str(int(time.time() * 1000))
        sc = int(time.time()) % 60
        while len(params.keys()) < random_int - 1:
            key = str(hex(int(random.random() * 1000000000)))[2:8]
            if key in params: continue
            params[key] = random.randrange(0, sc)
        sn = SUID[p:random_int + 1]
        uurl = "http://weixin.sogou.com/antispider/detect.php?sn=%s" % sn
        for key, value in params.items():
            uurl += "&%s=%s" % (key, value)
        # uurl+="&_="+mt
        theaders = dict(headers)
        theaders["X-Requested-With"] = "XMLHttpRequest"
        theaders["Accept"] = "application/json, text/javascript, */*; q=0.01"
        referer = conn.request.url
        theaders["Referer"] = referer
        if "Cookie" in theaders:
            theaders.pop("Cookie")
        conn = sreq.request_url(uurl, headers=theaders)
        content = conn.text.encode("utf-8")
        j = json.loads(content)
        if j["code"] != 0:
            raise RuntimeError("Get snuid error.")
        snuid = json.loads(content)["id"]

        setattr(self._tls, "snuid", snuid)
        headers["Cookie"] = "SNUID=" + snuid
        headers["Referer"] = referer

        # 获取SUV
        conn = sreq.request_url(
            "http://pb.sogou.com/pv.gif?uigs_productid=webapp&type=antispider&subtype=verify_page&domain=weixin&suv=&snuid=&t=%s&pv"
            % (str(int(time.time() * 1000))), headers=headers)
        setattr(self._tls, "sreq", sreq)
        return sreq

    def run_job(self,job,**kwargs):
        sreq = getattr(self._tls, "sreq", None)
        if not sreq:
            sreq = SessionRequests()
            sreq.request_url("http://weixin.sogou.com/")
            # sreq = BasicRequests()
            setattr(self._tls, "sreq", None)
        headers = {
            "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.59 Safari/537.36",
            "Connection":"keep-alive",
            "Upgrade-Insecure-Requests":"1",
        }
        if getattr(self._tls, "snuid", ""):
            headers["Cookie"]="SNUID="+getattr(self._tls, "snuid", "")

        if job["type"]=="u1":
            key = job["key"]
            url = "http://weixin.sogou.com/weixin?type=1&query=%s&ie=utf8&_sug_=n&_sug_type_="%key
            conn = sreq.request_url(url=url, headers=headers)
            if not conn or conn.code!=200:
                raise RuntimeError("search %s failed."%key)
            content = conn.text.encode("utf-8")
            if "sogou.com/antispider" in conn.request.url:
                Log.error("IP blocked. Try to fix...")
                sreq = self.rebuild_req(sreq=sreq,conn=conn,headers=headers)
                conn = sreq.request_url(url=url, headers=headers)
                if not conn or conn.code != 200:
                    raise RuntimeError("search %s failed." % key)
                content = conn.text.encode("utf-8")

            ret = dict()
            ret["key"] = key
            ret["result"] = list()

            if "抱歉!</strong>暂无与" in content:
                return ret
            accs = re.findall('div target="_blank" href="(.*?)"', content, re.S)
            if len(accs)==0:
                raise RuntimeError("extract %s's main page url failed."%key)

            for acc in accs:
                sub_url = acc.replace("&amp;","&")
                headers["Referer"] = url
                conn = sreq.request_url(url=sub_url, headers=headers)
                if not conn or conn.code!=200:
                    raise RuntimeError("get %s's main page failed."%key)
                content = conn.text.encode("utf-8")
                if "为了保护你的网络安全，请输入验证码" in content:
                    raise RuntimeError("Require Verification.")
                m = re.search(r"var msgList = '(.*?)';", content, re.S)
                if not m:
                    raise RuntimeError("get %s's article list failed."%key)
                msg_list_string = m.group(1)
                msg_list = json.loads(msg_list_string.replace("&quot;", '"').replace("\\", "").replace("&amp;amp;", "&"))["list"]
                data = dict()
                data["msg_list"]=list()
                if key == "hbskao":
                    print "here"
                for msg in msg_list:
                    #提取今日文章
                    if msg["comm_msg_info"]["datetime"]/(60*60*24)==int(time.time())/(60*60*24):
                        for item in msg["app_msg_ext_info"]["multi_app_msg_item_list"]:
                            data["msg_list"].append(item)
                        msg["app_msg_ext_info"].pop("multi_app_msg_item_list")
                        data["msg_list"].append(msg["app_msg_ext_info"])
                m = re.search("微信号:\s*(.*?)\s*<", content, re.S)
                data["account"]=m.group(1) if m else ""
                m = re.search('<strong class="profile_nickname">\s*(.*?)\s*</strong>', content, re.S)
                data["name"]=m.group(1) if m else ""
                new_job = {"msg_list":data["msg_list"],"type":"u2", "key":key, "name":data["name"],
                           "account":data["account"]}
                self.queue_manager.put_normal_job(new_job)
                ret["result"].append(data)
            return ret

        elif job["type"]=="u2":
            key = job["key"]
            msg_list = job["msg_list"]
            for msg in msg_list:
                app_msg_ext_info = msg["app_msg_ext_info"]
                content_url = app_msg_ext_info["content_url"]
                url = "http://mp.weixin.qq.com"+content_url
                conn = sreq.request_url(url = url, headers=headers)
                if not conn or conn.code != 200:
                    raise RuntimeError("get %s's article <<%s>> failed. url:%s"%(key,app_msg_ext_info["title"],content_url))
                msg["page"]=conn.text.encode("utf-8")
                msg["name"],msg["account"],msg["key"]=job["name"],job["account"],job["key"]
                msg["title"]=app_msg_ext_info["title"]
                self.saver.save_article_detail(data=msg,url=url)
if __name__ == "__main__":
    import spider.util
    spider.util.use_utf8()
    s = SogouWeiXinSpider(1)
    s.run()





