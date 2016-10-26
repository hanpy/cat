#!/usr/bin/env python
# encoding:utf-8

from spider.httpreq import SessionRequests
import random
import time
import spider.util
import uuid
from spider.runtime import Log
from lxml import html
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
        self.abs_path = "./"
        self.relative_path = "img/"

        self.dispatch_file = ""


    def parse(self, url, get_page=False):
        #url = "http://www.jyeoo.com/math/ques/partialques?q=75a08844-6562-4bf5-a182-034cf7929588~4e1c9a08-d989-45c8-b89f-097da57cbd75~&f=0&ct=2&dg=3&fg=16&po=0&pd=1&pi=1&r=0.6531368879266407"
        text = self.while_request(url, req_retry=5)
        all = re.findall(ur'<fieldset.*?>(.*?)</fieldset>(<span class="fieldtip">.*?下载</a></span>)', text, re.S)
        ret = list()
        if len(all) == 0:
            Log.error("没有匹配到任何题目相关.%s"%url)
            #这种要做个记录，后续手动查看为什么没有匹配到，是不是正则有问题还是确实没有？！
            return ret
        for div, span in all:
            # div 是题目和选项　span是查看解析/难度/真题/组卷
            #print "原始题干DIV:", div
            #print "原始查看解析/难度SPAN:", span
            result = {}
            #匹配获取题目和选项
            allpt = re.findall(ur'<!--B\d+-->(.*?)<!--E\d+-->', div, re.S)
            if len(allpt) > 0:
                question = allpt[0].replace('<span class="qseq"></span>', '')
                # 把　<a target="_blank" href="http://www.jyeoo.com/math/report/detail/6ff95e3b-603c-4231-95e7-17ad76d0fc9e">（2013•永州）</a> 转换成　（2013•永州）
                m = re.search(ur'<a.*?href="http://www\.jyeoo\.com/.*?".*?>(（.*?•.*?）)</a>', div, re.S)
                if m:
                    content = m.group(1)
                    result["zhentidiqu"] = content
                    repl = m.group()
                    question = question.replace(repl, content)
                # 匹配所有图片,并下载图片,以UUID命名保存在相对路径下,图片字段多张图片使用逗号拼接
                allimg = re.findall(ur'<img.*?src="(.*?\.png)".*?>', question, re.S)
                imgpaths = ""
                for imgurl in allimg:
                    imgcontent = self.while_request(imgurl, req_retry=5, img=True)
                    fname = self.relative_path + uuid.uuid3(uuid.NAMESPACE_DNS, imgurl.encode("utf-8")).get_hex() + ".png" #hashlib.md5(imgurl).hexdigest() + ".png"
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
            ret.append(result)
        print "总共拿到", len(ret), "条数据......", url
        if get_page:
            #<td style="text-align:right">共计<em style="color:red"  id='pchube' value='1'>480</em>道相关试题</td>
            m2 = re.search(ur'<td style="text-align:right">共计<em.*?>(\d+)</em>道相关试题</td>', text, re.S)
            if m2:
                allnum = int(m2.group(1))
                sub = allnum % 10
                return ret, allnum/10 if sub == 0 else allnum/10+1
        return ret



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
            get_img = kwargs.get('img', False)
            if get_img:
                return res.content
            return res.text

    def save_file(self, fname, content):
        with open(fname, 'wb') as f:
            f.write(content)

import spider.util
if __name__ == '__main__':
    spider.util.use_utf8()
    CurlReq.DEBUGREQ = 1
    n = Parser()
    n.parse("http://www.jyeoo.com/math/ques/partialques?q=a246fec1-afbc-42c4-a872-0d5bfd5308fb~94e9e22a-536b-41b4-8b8e-1ed235cfac0e~I9&f=0&ct=0&dg=0&fg=0&po=0&pd=1&pi=1&r=0.8556708036907169", get_page=True)