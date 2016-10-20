#! /usr/bin/env python
# encoding=utf-8
import re
import urllib
import urllib2
import cookielib

import sys


class MLogin():
    def __init__(self):
        self.cookie = cookielib.CookieJar()
        self.handler = urllib2.HTTPCookieProcessor(self.cookie)
        self.opener = urllib2.build_opener(self.handler)
        self.username = raw_input("请输入用户名:")
        self.first_pwd = raw_input("请输入一级密码:")
        self.second_pwd = raw_input("请输入二级密码:")

        self.headers={
            "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:49.0) Gecko/20100101 Firefox/49.0"
        }

    def request_url(self, url, data=None):
        self.opener.addheaders = self.get_headers()
        datastr = None if not data else urllib.urlencode(data)
        return self.opener.open(url,datastr)

    def get_headers(self):
        ret = list()
        for key,value in self.headers.items():
            ret.append((key,value))
        return ret

    def extract_param(self,key, content):
        m = re.search('name="%s"\s*value\s*=\s*"(.*?)"'%key, content, re.S)
        if not m:
            raise RuntimeError("无法解析参数"+key)
        return m.group(1)

    def login(self):
        opener = self.opener
        # 访问第一次，获取JSESSIONDD
        init_url = 'https://ipin.siren24.com/i-PINM2/jsp/ipin2_j21.jsp?id=86896E62A25FB6A41157DE4C5B9DF9E1&reqNum=020201D5F6ABAE130E196F74323599D6&retUrl=23http://www.siren24.com/mysiren/member/auth/auth02p_IpinResult.jsp&ipSeq=&domSeq=7805FC8599D3C93B658CAF8D8DBC08C5&urlSeq=DB3255CAF5E621672E4306B34C86EA09&addParam=|SIR001|GCT01'
        response = self.request_url(init_url)

        # 读取并处理韩文编码,解析出表单隐藏字段
        content = response.read().decode("euc-kr")
        pInfo = self.extract_param("pInfo", content)
        retUrl = self.extract_param("retUrl", content)
        addParam= self.extract_param("addParam", content)

        # 带着Cookie获取验证码并保存
        response2 = self.request_url('https://ipin.siren24.com/stickyCaptcha')
        with open("captch.png", "w") as f:
            f.write(response2.read())
        verify_code = raw_input("请输入验证码:")
        data = {"cbaId": self.username, "cbaPw": self.first_pwd, "captchaAnswer_enc": verify_code,
                "pInfo": pInfo, "retUrl": retUrl, "addParam": addParam}
        self.headers['Referer'] =init_url
        response3 = self.request_url('https://ipin.siren24.com/i-PINM2/jsp/ipin2_j21_dup.jsp', data)
        content3 = response3.read().decode("euc-kr")

        if "자동인증 방지 문자 입력값이 올바르지 않습니다" in content3:
            print '验证码错误'
            return
        if "비밀번호 1회 오류" in content3:
            print "密码或用户名出错"
            return

        #继续解析参数以供下次post
        data=dict()
        data["id"] = self.extract_param("id",content3)
        data["reqNum"] = self.extract_param("reqNum", content3)
        data["retUrl"] = self.extract_param("retUrl", content3)
        data["ipSeq"] = self.extract_param("ipSeq", content3)
        data["domSeq"] = self.extract_param("domSeq", content3)
        data["urlSeq"] = self.extract_param("urlSeq", content3)
        data["addParam"] = self.extract_param("addParam", content3)
        data["cbaId"] = self.extract_param("cbaId", content3)
        data["cbaPw"] = self.extract_param("cbaPw", content3)
        data["jobInfoValue"]= self.extract_param("jobInfoValue",content3)
        data["OrgCode"] = self.extract_param("OrgCode",content3)
        data["pInfo"] = self.extract_param("pInfo", content3)

        self.headers['Referer'] ='https://ipin.siren24.com/i-PINM2/jsp/ipin2_j21_dup.jsp'
        response4 = self.request_url("https://ipin.siren24.com/i-PINM2/jsp/ipin2_j22_01.jsp",data)
        content4 = response4.read().decode("euc-kr")
        #还要解析参数
        data = dict()
        data["IDPCODE"] = self.extract_param("IDPCODE", content4)
        data["CPCODE"] = self.extract_param("CPCODE", content4)
        data["CPREQUESTNUM"] = self.extract_param("CPREQUESTNUM", content4)
        data["RETURNURL"] = self.extract_param("RETURNURL", content4)
        data["IDPURL"] = self.extract_param("IDPURL", content4)
        data["AUTHIDINFO"] = self.extract_param("AUTHIDINFO", content4)

        self.headers['Referer']='https://ipin.siren24.com/i-PINM2/jsp/ipin2_j22_01.jsp'
        response5 = self.request_url("https://cert.vno.co.kr/in.cb", data)

        self.headers['Referer'] ='https://cert.vno.co.kr/in.cb'
        response6 = self.request_url("https://cert.vno.co.kr/IPIN/inlogin.cb")
        content6 = response6.read().decode("euc-kr")
        data = dict()
        data["CLIENTKEY"] = self.extract_param("CLIENTKEY", content6)
        data["ukey"] = self.extract_param("ukey", content6)
        data["rkey"] = self.extract_param("rkey", content6)
        data["passwd"] = self.second_pwd #二次密码

        self.headers['Referer'] ='https://cert.vno.co.kr/IPIN/inlogin.cb'
        response7 =self.request_url("https://cert.vno.co.kr/IPIN/add_auth_login_proc.cb", data)
        content7 = response7.read().decode("euc-kr")
        if "입력하신 2차비밀번호가 1회 일치하지 않습니다" in content7:
            print "登陆失败，提示二次密码不正确"
        if "지금변경하기" in content7:
            #提示更改密码,判断为登录成功
            print "登陆成功"
            print content7





if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')
    s = MLogin()
    s.login()
