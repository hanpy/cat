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
华夏基金:
登陆用户名密码均为加密，已经做完
http://www.chinaamc.com/wodejijin/zhcx/index.shtml

GET https://www.chinaamc.com/portal/cn/query/image.jsp?id=0.3829123220789441 HTTP/1.1
Host: www.chinaamc.com
User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0
Accept: */*
Accept-Language: zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3
Accept-Encoding: gzip, deflate, br
Referer: https://www.chinaamc.com/portal/cn/forZcms/login_cms.jsp
Connection: keep-alive


POST https://www.chinaamc.com/portal/webuserlogin.esp HTTP/1.1
Host: www.chinaamc.com
User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8
Accept-Language: zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3
Accept-Encoding: gzip, deflate, br
Referer: https://www.chinaamc.com/portal/cn/forZcms/login_cms.jsp
Cookie: portalCookie=hmJcYN8ccQp0vhYqXn2KpwpBshNBmLvhvbYLxq026NlPSwngDvkp!2042055975; hxjjCookie=69212170.20480.0000; zcmsCookie=hnqmYN8MYllTF1L1bYdgpJBYlTkrlqKTrjVzHytBv2ckDXC7thGh!2042055975
Connection: keep-alive
Upgrade-Insecure-Requests: 1
Content-Type: application/x-www-form-urlencoded
Content-Length: 82

catalogid=&articleid=&num=08365&username=036000423383&password=588502&number=08365

"""

class HuaXiaJiJin():
    def __init__(self, username, password):
        self.req = SessionRequests()
        self.req.select_user_agent("=Mozilla/5.0 (Windows NT 10.0; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0")
        self.username = username
        self.password = password

    def save_file(self, fname, content):
        with open(fname, 'wb') as f:
            f.writelines(content)

    def get_img(self):
        url = "https://www.chinaamc.com/portal/cn/query/image.jsp?id=" + str(random.random())
        con = self.req.request_url(url)
        if con is None or con.code != 200:
            print  "拉取验证码:", "con is None " if con is None else "con.code = %d " % con.code
        else:
            imgcontent = con.content
            self.save_file("captcha.jpg", imgcontent)
            print "验证码图片保存为captcha.jpg..."

    def login(self):
        self.get_img()
        captcha_code = raw_input("请输入验证码captcha.jpg:")
        url = "https://www.chinaamc.com/portal/webuserlogin.esp"
        headers = {"Referer": "https://www.chinaamc.com/portal/cn/forZcms/login_cms.jsp",
                   "Upgrade-Insecure-Requests": "1",
                   "Content-Type": "application/x-www-form-urlencoded"}
        data = {"catalogid":"",
                "articleid": "",
                "num": captcha_code,
                "username": self.username,
                "password": self.password,
                "number":captcha_code
                }
        con = self.req.request_url(url, data=data, headers=headers)
        if con is None or con.code != 200:
            print "登陆:", "con is None " if con is None else "con.code = %d " % con.code
        else:
            text = con.content.decode('gb18030')#.encode('utf-8')
            if u"<title>华夏基金账户查询页面</title>" in text:
                print "登陆成功============================================================\n"
            else:
                print "登陆失败============================================================\n", text

        self.jiaojimingxi()

    """
    GET https://www.chinaamc.com/portal/cn/query/jymx.jsp?sonpathid=100714 HTTP/1.1
    Host: www.chinaamc.com
    User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0
    Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8
    Accept-Language: zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3
    Accept-Encoding: gzip, deflate, br
    Referer: https://www.chinaamc.com/portal/dyindex.esp?pathid=100708&sonpathid=100714&WT.ac=kfwscx_kstd_jymxcx
    Cookie: portalCookie=yQL6YN1BLNphQTSTdJRjpLyqtTBNJnjyr2vZSzCZBFp79mqyLzrH!2042055975; zcmsCookie=zGLnYN1Ts1Tlp8BlJcJrLQ4SnvKvywJtSY2TTKFmhndcDJdnJxHt!2042055975; hxjjCookie=69212170.20480.0000; zhcxcustid=sqpwqwpyqqqquuqxxtxs; WT_FPC=id=2819f55249a7efe62741481457763669:lv=1481458372003:ss=1481457763669
    Connection: keep-alive
    Upgrade-Insecure-Requests: 1
    """
    def jiaojimingxi(self):
        url = "https://www.chinaamc.com/portal/cn/query/jymx.jsp?sonpathid=100714"
        con = self.req.request_url(url)
        if con is None or con.code != 200:
            print "查询交易明细:", "con is None " if con is None else "con.code = %d " % con.code
        else:
            text = con.content.decode("gb18030")
            if u'<title>交易明细</title>' in text:
                print "查询交易明细成功..."
            else:
                print "查询交易明细失败..."

if __name__ == "__main__":
    username = "036000423383"
    pwd = "588502"
    h = HuaXiaJiJin(username, pwd)
    h.login()






