#!/usr/bin/env python
# encoding:utf-8

from spider.httpreq import SessionRequests
from spider.spider import Spider
import random
import time
import spider.util
import json
import uuid
bloom = set()
from spider.savebin import FileSaver
import threading
from lxml import html
import hashlib
import re
from spider.httpreq import CurlReq

class Parser():
    def __init__(self):
        self.proxies_dict = [
                             {'http': 'http://ipin:ipin1234@120.55.97.254:18888', 'https': 'https://ipin:ipin1234@120.55.97.254:18888'},
                             {'http': 'http://ipin:ipin1234@120.26.111.91:18888', 'https': 'https://ipin:ipin1234@120.26.111.91:18888'},
                             {'http': 'http://ipin:ipin1234@120.26.52.20:18888', 'https': 'https://ipin:ipin1234@120.26.52.20:18888'},
                             {'http': 'http://ipin:ipin1234@120.26.134.183:18888', 'https': 'https://ipin:ipin1234@120.26.134.183:18888'},
                             {'http': 'http://ipin:ipin1234@120.55.92.132:18888', 'https': 'https://ipin:ipin1234@120.55.92.132:18888'}
                             ]
        self.req = SessionRequests()
        self.abs_path = "/home/windy/selfwork/jyeoo"
        self.relative_path = "/img/"

        self.dispatch_file = ""


    def parse(self, text):
        #url = "http://www.jyeoo.com/math/ques/partialques?q=75a08844-6562-4bf5-a182-034cf7929588~4e1c9a08-d989-45c8-b89f-097da57cbd75~&f=0&ct=9&dg=2&fg=8&po=0&pd=1&pi=1&r=0.7957780709619277"
        #url = "http://www.jyeoo.com/math/ques/partialques?q=75a08844-6562-4bf5-a182-034cf7929588~4e1c9a08-d989-45c8-b89f-097da57cbd75~&f=0&ct=1&dg=2&fg=8&po=0&pd=1&pi=1&r=0.8630216340591749"
        #url = "http://www.jyeoo.com/math/ques/partialques?q=75a08844-6562-4bf5-a182-034cf7929588~4e1c9a08-d989-45c8-b89f-097da57cbd75~&f=0&ct=1&dg=3&fg=16&po=0&pd=1&pi=1&r=0.30025958855188817"
        url = "http://www.jyeoo.com/math/ques/partialques?q=75a08844-6562-4bf5-a182-034cf7929588~4e1c9a08-d989-45c8-b89f-097da57cbd75~&f=0&ct=2&dg=3&fg=16&po=0&pd=1&pi=1&r=0.6531368879266407"
        text = self.while_request(url, req_retry=5)
        all = re.findall(ur'<fieldset.*?>(.*?)</fieldset>(<span class="fieldtip">.*?下载</a></span>)', text, re.S)
        if len(all) == 0:
            print "没有匹配到任何题目相关......"
            #这种要做个记录，后续手动查看为什么没有匹配到，是不是正则有问题还是确实没有？！
            return
        cnt = 0
        for div, span in all:
            # div 是题目和选项　span是查看解析/难度/真题/组卷
            #print "原始题干DIV:", div
            #print "原始查看解析/难度SPAN:", span
            result = {}
            #div = u"""<div class="pt1"><!--B1--><span class="qseq"></span><a href="http://www.jyeoo.com/math/report/detail/915fa26b-f7e3-4cb1-8310-f905288c97c3" target="_blank">（2016春•张掖校级月考）</a>如图是一个计算程序，若输入的值为-1，则输出的结果应为<!--BA--><div class='quizPutTag' contenteditable='true'>&nbsp;</div><!--EA-->．<br /><img alt="菁优网" src="http://img.jyeoo.net/quiz/images/201512/254/ce389444.png" style="vertical-align:middle" /><br /><img alt="菁优网" src="http://img.jyeoo.net/quiz/images/201512/254/ce389445.png" style="vertical-align:middle" /><!--E1--></div><div class="pt6" style="display:none"></div>"""
            #匹配获取题目和选项
            allpt = re.findall(ur'<!--B\d+-->(.*?)<!--E\d+-->', div, re.S)
            if len(allpt) > 0:
                question = allpt[0].replace('<span class="qseq"></span>', '')
                # 把　<a target="_blank" href="http://www.jyeoo.com/math/report/detail/6ff95e3b-603c-4231-95e7-17ad76d0fc9e">（2013•永州）</a> 转换成　（2013•永州）
                m = re.search(ur'<a.*?href="http://www\.jyeoo\.com/.*?".*?>(（.*?•.*?）)</a>', div, re.S)
                if m:
                    content = m.group(1)
                    repl = m.group()
                    question = question.replace(repl, content)
                # 匹配所有图片,并下载图片,以UUID命名保存在相对路径下,图片字段多张图片使用逗号拼接
                allimg = re.findall(ur'<img.*?src="(.*?\.png)".*?>', question, re.S)
                imgpaths = ""
                for imgurl in allimg:
                    imgcontent = self.while_request(imgurl, req_retry=5)
                    fname = self.relative_path + uuid.uuid3(uuid.NAMESPACE_DNS, imgurl) + ".png" #hashlib.md5(imgurl).hexdigest() + ".png"
                    imgpaths += fname + ","
                    self.save_file(self.abs_path + fname, imgcontent)
                    question = question.replace(imgurl, fname)
                question = question.replace(u'alt="菁优网"', '')
                result["tupian"] = imgpaths[0:-1]
                result["tigan"] = question
                #print question
                # 选择题 有选项　直接保存选项源码
                if len(allpt) > 1:
                    result["xuanxiang"] = allpt[1].strip()
            else:
                print "没有匹配到题干...", url


            dom = html.fromstring(span)
            lables = dom.xpath("//label")
            if len(lables) == 3:
                result["nandu"] = lables[0].text_content().strip()
                result["zhenti"] = lables[1].text_content().strip()
                result["zujuan"] = lables[2].text_content().strip()
            else:
                print "找不到难度/真题/组卷......", url

            tagA = dom.xpath("//a[@target='_blank']")
            if len(tagA) > 0:
                href = tagA[0].attrib.get("href", '')
                result["href"] = href
                self.parse_detail(href, result)
            else:
                print "拿不到详情页面链接...", url
            print "结果-->", spider.util.utf8str(result)
            cnt += 1
        print "总共拿到", cnt, "条数据......", url



    def parse_detail(self, href, result):
        # 解析详情页面　考点　分析　解答　点评
        text = self.while_request(href, req_retry=5)

        detail_dom = html.fromstring(text)
        pt3 = detail_dom.xpath("//div[@class='pt3']")
        if len(pt3) > 0:
            result["kaodian"] = pt3[0].text_content().strip()

        pt5 = re.findall(ur'<div class="pt5"><!--B5-->(.*?)<!--E5--></div>', text, re.S)
        if len(pt5) > 0:
            fenxi = pt5[0]
            result["fenxi"] = fenxi

        pt6 = re.findall(ur'<div class="pt6"><!--B6-->(.*?)<!--E6--></div>', text, re.S)
        if len(pt6) > 0:
            jieda = pt6[0]
            result["jieda"] = jieda

        pt7 = re.findall(ur'<div class="pt7"><!--B7-->(.*?)<!--E7--></div>', text, re.S)
        if len(pt7) > 0:
            dianping = pt7[0]
            result["dianping"] = dianping


    def while_request(self, url, **kwargs):
        retry = kwargs["req_retry"] if "req_retry" in kwargs else 1
        i = 0
        while i < retry:
            num = random.randint(0, len(self.proxies_dict) - 1)
            kwargs["proxies"] = self.proxies_dict[num]
            res = self.req.request_url(url, **kwargs)
            if res is None or res.code != 200:
                print "con is None " if res is None else "con.code = %d " % res.code, url
                time.sleep(random.randint(1, 3))
                i += 1
                continue
            return res.text

    def save_file(self, fname, content):
        with open(fname, 'wb') as f:
            f.writelines(content)


if __name__ == '__main__':
    CurlReq.DEBUGREQ = 1
    n = Parser()
    n.parse("")