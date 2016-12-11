#! /usr/bin/env python
# encoding:utf-8

"""
@author:chentao
@time: 2016/12/11 16:17
"""
import copy
import json
import os
import random
import re
import time

from spider.httpreq import SessionRequests
"""
工银瑞信  -----没开始做
初试：用户名密码均通过JS加密
http://www.icbccs.com.cn/cif/login.jsp

验证码：
GET http://www.icbccs.com.cn/cif/RandomCodeCtrl?0.9005175561326979 HTTP/1.1
Host: www.icbccs.com.cn
User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0
Accept: */*
Accept-Language: zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3
Accept-Encoding: gzip, deflate
Referer: http://www.icbccs.com.cn/cif/login.jsp
Connection: keep-alive


POST http://www.icbccs.com.cn/cif/MainCtrl HTTP/1.1
Host: www.icbccs.com.cn
User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8
Accept-Language: zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3
Accept-Encoding: gzip, deflate
Content-Type: application/x-www-form-urlencoded
Referer: http://www.icbccs.com.cn/cif/login.jsp
Content-Length: 198
Cookie: JSESSIONID=0000VGi_mLpWhggqftzP7Gbu9A5:-1; TSdc3889=5ddadf104d3661707bdba47a132ebd75a0a8fea938381bea584d486960ac0ec5b5e5a3b7
Connection: keep-alive

undefined&page=FundLoginPage&url=null&certificate_no=NDg2Mzc3NTEwMDEx&certificate_no=&passwd=MDEwMDEw&passwd=&certify=mtnp&a=%E7%99%BB%20%E5%BD%95&b=%E9%87%8D%20%E5%A1%AB&sel_type=x&_isAjaxRequest=y
"""

class GongYinRuiXin():
    def __init__(self, username, password):
        self.req = SessionRequests()
        self.req.select_user_agent("=Mozilla/5.0 (Windows NT 10.0; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0")
        self.username = username
        self.password = password

    def request(self, url ,**kwargs):
        con = self.req.request_url(url, **kwargs)
        if con is None or con.code != 200:
            print  "request:", "con is None " if con is None else "con.code = %d " % con.code
        else:
            print "链接:", url, "请求成功!!"
            if 'img' in kwargs:
                return con.content
            return con.text

    def init_req(self):
        self.request("http://www.icbccs.com.cn/cif/login.jsp")

    def save_file(self, fname, content):
        with open(fname, 'wb') as f:
            f.writelines(content)

    def get_img(self):
        content = self.request("http://www.icbccs.com.cn/cif/RandomCodeCtrl?" + str(random.random()), img='img')
        self.save_file("captcha.jpg", content)
        print "captcha.jpg save success !"

    def login(self):
        self.get_img()
        url = "http://www.icbccs.com.cn/cif/MainCtrl"
        headers = {"Referer": "http://www.icbccs.com.cn/cif/login.jsp",
                   "Content-Type": "application/x-www-form-urlencoded"}
        captcha_code = raw_input("请输入验证码captcha.jpg:")
        data = {"undefined":"",
                "page": "FundLoginPage",
                "url": "null",
                "certificate_no": "NDg2Mzc3NTEwMDEx",
                "certificate_no": "",
                "passwd": "MDEwMDEw",
                "passwd": "",
                "certify": captcha_code,
                "a": "%E7%99%BB%20%E5%BD%95",
                "b": "%E9%87%8D%20%E5%A1%AB",
                "sel_type": "x",
                "_isAjaxRequest": "y"
                }
        text = self.request(url, headers=headers, data=data)
        if u'jsp/fund_user/welcome.jsp' == text:
            print "登陆成功..."
        else:
            print "登陆失败...\n", text


if __name__ == "__main__":
    username = "486377510011"
    pwd = "010010"
    h = GongYinRuiXin(username, pwd)
    h.login()






