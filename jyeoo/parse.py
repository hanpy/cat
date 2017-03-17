#!/usr/bin/env python
# encoding:utf-8
import difflib

from spider.ProxyPool import ADSLProxyPool
from spider.httpreq import SessionRequests, BasicRequests
import random
import time
import spider.util
import uuid
from spider.runtime import Log
from lxml import html
import re
from spider.httpreq import CurlReq
from spider.CommonSpider import CommonSpider

g_user_agent = ["=Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1",
                "=Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0",
                "=Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
                "=Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.9.168 Version/11.50",
                "=Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; Tablet PC 2.0; .NET4.0E)",
                "=Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; InfoPath.3)",
                "=Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; GTB7.0)",
                "=Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
                "=Mozilla/5.0 (Windows; U; Windows NT 6.1; ) AppleWebKit/534.12 (KHTML, like Gecko) Maxthon/3.0 Safari/534.12",
                "=Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E)",
                "=Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E; SE 2.X MetaSr 1.0)",
                "=Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.472.33 Safari/534.3 SE 2.X MetaSr 1.0",
                "=Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E)",
                "=Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.41 Safari/535.1 QQBrowser/6.9.11079.201",
                "=Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E) QQBrowser/6.9.11079.201"]

class Parser():
    def __init__(self, proxy_pool=None, sreq=None):
        self.proxies_dict = [
                             {'http': 'http://ipin:ipin1234@120.55.97.254:18888', 'https': 'https://ipin:ipin1234@120.55.97.254:18888'},
                             {'http': 'http://ipin:ipin1234@120.26.111.91:18888', 'https': 'https://ipin:ipin1234@120.26.111.91:18888'},
                             {'http': 'http://ipin:ipin1234@120.26.52.20:18888', 'https': 'https://ipin:ipin1234@120.26.52.20:18888'},
                             {'http': 'http://ipin:ipin1234@120.26.134.183:18888', 'https': 'https://ipin:ipin1234@120.26.134.183:18888'},
                             {'http': 'http://ipin:ipin1234@120.55.92.132:18888', 'https': 'https://ipin:ipin1234@120.55.92.132:18888'}
                             ]
        self.req = sreq or SessionRequests()
	self.proxy_pool = proxy_pool
        self.abs_path = "./"
        self.relative_path = "img/"

        self.dispatch_file = ""


    def parse(self, url, job={}, get_page=False):
        #url = "http://www.jyeoo.com/math/ques/partialques?q=75a08844-6562-4bf5-a182-034cf7929588~4e1c9a08-d989-45c8-b89f-097da57cbd75~&f=0&ct=2&dg=3&fg=16&po=0&pd=1&pi=1&r=0.6531368879266407"
        text = self.while_request(url, req_retry=5)
        all = re.findall(ur'<fieldset.*?>(.*?)</fieldset>(<span class="fieldtip">.*?下载</a></span>)', text, re.S)
        ret = list()
        if re.search(ur'访问太快', text, re.S):
            Log.error("访问太快")
            raise RuntimeError("访问太快")
        if len(all) == 0:
            Log.error("没有匹配到任何题目相关.%s"%url)
            raise CommonSpider.BadJobError("没有匹配到任何题目相关.%s"%url)
            #这种要做个记录，后续手动查看为什么没有匹配到，是不是正则有问题还是确实没有？！
            #if get_page:
            #    return ret, 0
            #return ret
        for div, span in all:
            # div 是题目和选项　span是查看解析/难度/真题/组卷
            #print "原始题干DIV:", div
            #print "原始查看解析/难度SPAN:", span
            result = {"kaodian":"","nandu":"","tigan":"","tupian":"","xuanxiang":"", "xuanxiangtupian":"",
                      "zhenti":"","zhentidiqu":"","zujuan":"","fenxi":"","jieda":"",
                      "dianping":""}
            #匹配获取题目和选项
            allpt = re.findall(ur'<!--B\d+-->(.*?)<!--E\d+-->', div, re.S)
            if len(allpt) > 0:
                result["raw_data"] = allpt[0]
                question = allpt[0].replace('<span class="qseq"></span>', '')
                # 把　<a target="_blank" href="http://www.jyeoo.com/math/report/detail/6ff95e3b-603c-4231-95e7-17ad76d0fc9e">（2013•永州）</a> 转换成　（2013•永州）
                m = re.search(ur'<a.*?href="http://www\.jyeoo\.com/.*?".*?>(（.*?•.*?）)</a>', div, re.S)
                if m:
                    content = m.group(1)
                    result["zhentidiqu"] = content
                    repl = m.group()
                    question = question.replace(repl, content)
                # 匹配所有图片,并下载图片,以UUID命名保存在相对路径下,图片字段多张图片使用逗号拼接
                imgpaths = result["tupian"]
                question, imgpaths = self.img_process(question, imgpaths)
                result["tupian"] = imgpaths
                result["tigan"] = question
                #print question
                # 选择题 有选项　直接保存选项源码
                if len(allpt) > 1:
                    options = allpt[1].strip()
                    imgpaths = result["tupian"]
                    options, imgpaths = self.img_process(options, imgpaths)
                    result["tupian"] = imgpaths
                    result["xuanxiang"] = options
            else:
                job["err_msg"] = "没有匹配到题干.div=#%s" % div
                raise CommonSpider.BadJobError()

            dom = html.fromstring(span)
            labeles = dom.xpath("//label")
            if len(labeles) == 3:
                result["nandu"] = labeles[0].text_content().split("：")[1].strip()
                result["zhenti"] = labeles[1].text_content().split("：")[1].strip()
                result["zujuan"] = labeles[2].text_content().split("：")[1].strip()
            else:
                job["err_msg"] = "找不到难度．真题．组卷.span=#%s" % span
                raise CommonSpider.BadJobError()

            tagA = dom.xpath("//a[@target='_blank']")
            if len(tagA) > 0:
                href = tagA[0].attrib.get("href", '')
                # TODO
                # self.parse_detail(href, result)
                result["href"] = href
            else:
                job["err_msg"] = "拿不到详情页面链接.span=#%s" % span
                raise CommonSpider.BadJobError()
            #print "结果-->", spider.util.utf8str(result)
            ret.append(result)
        print "总共拿到", len(ret), "条数据......", url
        m = re.search(r'"index cur">\s*(\d+)\s*<', text, re.S)
        if m:
            curr_page = int(m.group(1))
        elif len(ret) >0:
            curr_page = 1
        else:
            print text
            raise RuntimeError("需要登录")
        if get_page:
            #<td style="text-align:right">共计<em style="color:red"  id='pchube' value='1'>480</em>道相关试题</td>
            m2 = re.search(ur'<td style="text-align:right">共计<em.*?>(\d+)</em>道相关试题</td>', text, re.S)
            if m2:
                allnum = int(m2.group(1))
                sub = allnum % 10
                return ret, allnum/10 if sub == 0 else allnum/10+1, curr_page
        return ret, curr_page


    def img_process(self, imgtext, imgpaths):
        # 匹配所有图片,并下载图片,以UUID命名保存在相对路径下,图片字段多张图片使用逗号拼接
        allimg = re.findall(ur'<img.*?src="(.*?\.png)".*?>', imgtext, re.S)
        for imgurl in allimg:
            imgcontent = self.while_request(imgurl, req_retry=5, img=True)
            fname = self.relative_path + uuid.uuid3(uuid.NAMESPACE_DNS, imgurl.encode("utf-8")).get_hex() + ".png"
            imgpaths += fname + ","
            self.save_file(self.abs_path + fname, imgcontent)
            imgtext = imgtext.replace(imgurl, fname)
        imgtext = imgtext.replace(u'alt="菁优网"', '')
        return imgtext, imgpaths

    def parse_detail(self, href, result, job={}):
        # 解析详情页面　考点　分析　解答　点评
        text = self.while_request(href, req_retry=5)
        if 'class="mustvip"' in text:
            job["err_msg"] = "详情页需要vip账号才能查看#%s" % (href)
            raise CommonSpider.BadJobError()
        detail_dom = html.fromstring(text)

        curr = detail_dom.xpath("//div[@class='pt1']")[0].text_content().strip()
        tigan = html.fromstring(result["raw_data"].decode('utf8')).text_content().strip()
        radio = difflib.SequenceMatcher(None, curr, tigan).ratio()
        match = False
        if radio > 0.5:
            labeles = detail_dom.xpath("//span[@class='fieldtip']/label")
            if len(labeles) == 3:
                nandu = labeles[0].text_content().split("：")[1].strip()
                zhenti = labeles[1].text_content().split("：")[1].strip()
                zujuan = labeles[2].text_content().split("：")[1].strip()
                if nandu == result["nandu"] and zhenti == result["zhenti"] and zujuan == result["zujuan"]:
                    match = True
                else:
                    print "　parse_detail列表-->难度:%s,真题:%s,组卷:%s \n　parse_detail详情-->难度:%s,真题:%s,组卷:%s" % (result["nandu"], result["zhenti"], result["zujuan"], nandu, zhenti, zujuan)
            else:
                job["err_msg"] = "详情页找不到难度．真题．组卷.href=#%s" % href
                raise CommonSpider.BadJobError()
        else:
            print "文本匹配度:%d \n 列表页文本:%s \n 详情页文本:%s" % (radio, tigan, curr)

        if not match:
            time.sleep(3)
            raise RuntimeError("IP Blocked.")

        pt3 = detail_dom.xpath("//div[@class='pt3']")
        pt5 = re.findall('<div class="pt5"><!--B5-->(.*?)<!--E5--></div>', text, re.S)
        pt6 = re.findall('<div class="pt6"><!--B6-->(.*?)<!--E6--></div>', text, re.S)
        pt7 = re.findall('<div class="pt7"><!--B7-->(.*?)<!--E7--></div>', text, re.S)
        if len(pt3) > 0 and len(pt5) > 0 and len(pt6) > 0 and len(pt7) > 0:
            result["kaodian"] = pt3[0].text_content().strip()

            imgpaths = result["tupian"]

            fenxi = pt5[0]
            fenxi, imgpaths = self.img_process(fenxi, imgpaths)
            result["fenxi"] = fenxi

            jieda = pt6[0]
            jieda, imgpaths = self.img_process(jieda, imgpaths)
            result["jieda"] = jieda

            dianping = pt7[0]
            dianping, imgpaths = self.img_process(dianping, imgpaths)
            result["dianping"] = dianping

            result["tupian"] = imgpaths
        else:
            job["err_msg"] = "详情页找不到考点或分析或解答或点评#%s" % (href)
            raise CommonSpider.BadJobError()

    def while_request(self, url, **kwargs):
        retry = kwargs["req_retry"] if "req_retry" in kwargs else 1
        i = 0
        while i < retry:
            #num = random.randint(0, len(self.proxies_dict) - 1)
            kwargs["proxies"] = self.proxy_pool.get_one()
            kwargs["timeout"] = 6
            kwargs["proxy_credit"]=1
            self.req.select_user_agent(g_user_agent[random.randrange(0, len(g_user_agent))])
            res = self.req.request_url(url, **kwargs)
            # res = self.req.request_url(url, **kwargs)
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

    def remove_tag(self, content):
        m = re.compile("(<.*?>)")
        content = re.sub(m,"",content,)
        m = re.compile("(\(.*?\))")
        content = re.sub(m,"",content)
        m = re.compile("(（.*?）)")
        content = re.sub(m,"",content)
        return content


import spider.util
if __name__ == '__main__':
    spider.util.use_utf8()
    CurlReq.DEBUGREQ = 1
    proxy_pool = ADSLProxyPool()
    proxy_pool.run()
    n = Parser(proxy_pool=proxy_pool)
    n.parse("http://www.jyeoo.com/math/ques/partialques?q=a246fec1-afbc-42c4-a872-0d5bfd5308fb~94e9e22a-536b-41b4-8b8e-1ed235cfac0e~I9&f=0&ct=0&dg=0&fg=0&po=0&pd=1&pi=1&r=0.8556708036907169", get_page=True)
    proxy_pool.close()
    #r=dict()
    #n.parse_detail(href="http://www.jyeoo.com/math/ques/detail/105a9b16-1151-415c-52b8-94e25863ae49",result=r)
    #print r
