#! /usr/bin/env python
# encoding:utf-8

"""
@author:hanpy
@time: 2016/10/28 11:35
"""
import json
import random
import re

from spider.httpreq import SessionRequests

class JyeooRegist(SessionRequests):
    def __init__(self,acc ,pwd):
        SessionRequests.__init__(self)
        self.select_user_agent("firefox")
        self.acc = acc
        self.pwd = pwd

    def regist(self):
        conn = self.request_url("http://www.jyeoo.com/account/register")
        conn2 = self.request_url("http://www.jyeoo.com/account/checkeduser", data={"e": self.acc, "r": random.random()},
                                 headers={"Referer": "http://www.jyeoo.com/account/register",
                                          "X-Requested-With": "XMLHttpRequest"})
        ret = json.loads(conn2.text)
        if ret["M"] != "邮箱验证通过":
            print "邮箱验证未通过，该邮箱已有绑定的账号"
            return False
        AnonId = re.search('id="AnonID".*?value="(.*?)"', conn.text, re.S).group(1)
        token = re.search('name="__RequestVerificationToken".*?value="(.*?)"', conn.text, re.S).group(1)
        imgurl = re.search('name="Captcha".*?src="(.*?)"', conn.text, re.S).group(1)
        conn = self.request_url("http://www.jyeoo.com"+imgurl)
        with open("rcap.png","w") as f:
            f.write(conn.content)
        verify_code = raw_input("请输入验证码:")
        headers= {
            "Referer":"http://www.jyeoo.com/account/register"
        }
        data={
            "Email":self.acc,
            "Name":self.acc,
            "UT":3,
            "UG":7,
            "Password":self.pwd,
            "CPassword":self.pwd,
            "Captcha":verify_code,
            "RUrl":"http://space.jyeoo.com/",
            "Ver":True,
            "AnonID":AnonId,
            "UserID":"",
            "Source":"www",
            "__RequestVerificationToken":token
        }
        conn = self.request_url(url="http://www.jyeoo.com/account/register", data=data, headers=headers)
        if "'9' == '9'" in conn.text:
            print "注册成功"
            return True
        return False


class JyeooLogin(SessionRequests):
    def __init__(self, acc, pwd):
        SessionRequests.__init__(self)
        self.select_user_agent("firefox")
        self.acc = acc
        self.pwd = pwd

    def login(self):
        headers={"X-Requested-With":"XMLHttpRequest"}
        conn = self.request_url(url="http://www.jyeoo.com/math/api/iframelogin?t=&u=&r="+str(random.random()),
                         headers=headers)

        m = re.search('src=".*?&r=(.*?)"', conn.text, re.S)
        r = m.group(1)
        conn = self.request_url(url="http://www.jyeoo.com/account/loginform?t=&u=&r=%s"%r)
        m = re.search('<\s*input.*?id="AnonID".*?value="(.*?)"', conn.text, re.S)
        AnonID = m.group(1)
        m = re.search('<\s*img.*?id="capimg".*?src="(.*?)"', conn.text, re.S)
        img_url = "http://www.jyeoo.com"+m.group(1)
        img_content = self.request_url(img_url).content
        with open("cap.png","w") as f:
            f.write(img_content)
        verify_code = raw_input("请输入验证码:")
        data={"Email":self.acc,
              "Password":self.pwd,
              "Captcha":verify_code,
              "Remember":False,
              "Ver":True,
              "AnonID":AnonID,
              "Title":""
              }
        conn = self.request_url(url="http://www.jyeoo.com/account/loginform", data=data)
        if '“计算结果”错误' in conn.text:
            print "验证码错误"
            return False
        if 'top.parent.loginSuccess("00000000-0000-0000-0000-000000000000", "");' in conn.text:
            print "密码错误"
            return False
        print "登陆成功"
        self.isLogin = True
        return True

import spider.util
if __name__ == "__main__":
    spider.util.use_utf8()
    j = JyeooRegist("sssysu_hanpy@163.com","138138")
    j.regist()


