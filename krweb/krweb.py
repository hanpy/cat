#! /usr/bin/env python
# encoding=utf-8
import re
import traceback
import urllib
import urllib2
import cookielib

import sys

DEBUG = False

class MLogin(object):
    def __init__(self):
        self.init_content =""
        self.cookie = cookielib.CookieJar()
        self.handler = urllib2.HTTPCookieProcessor(self.cookie)
        self.opener = urllib2.build_opener(self.handler)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:49.0) Gecko/20100101 Firefox/49.0"
        }
        # self.headers["Cookie"]= "JSESSIONID=%s" % jsessionid

    def init_url(self):
        url = 'https://ipin.siren24.com/i-PINM2/jsp/ipin2_j21.jsp?id=86896E62A25FB6A41157DE4C5B9DF9E1&reqNum=020201D5F6ABAE130E196F74323599D6&retUrl=23http://www.siren24.com/mysiren/member/auth/auth02p_IpinResult.jsp&ipSeq=&domSeq=7805FC8599D3C93B658CAF8D8DBC08C5&urlSeq=DB3255CAF5E621672E4306B34C86EA09&addParam=|SIR001|GCT01'
        response = self.request_url(url)
        self.init_content = response.read().decode("euc-kr")
        if DEBUG:
            response1 = self.request_url('https://ipin.siren24.com/stickyCaptcha')
            with open("captch.png", 'w') as f:
                f.write(response1.read())
        return self.cookie._cookies["ipin.siren24.com"]["/"]["JSESSIONID"].value, response.read()

    def request_url(self, url, data=None):
        print "req.url: %s"%url
        self.opener.addheaders = self.get_headers()
        datastr = None if not data else urllib.urlencode(data)
        return self.opener.open(url, datastr)

    def get_headers(self):
        ret = list()
        for key, value in self.headers.items():
            ret.append((key, value))
        return ret

    def extract_param(self, key, content, msg=""):
        m = re.search('name="%s"\s*value\s*=\s*"(.*?)"' % key, content, re.S)
        if not m:
            raise RuntimeError("无法解析参数%s %s" % (key, msg))
        return m.group(1)

    def login(self,usename, pwd1, pwd2, verify_code):
        try:
            self._do_login(usename, pwd1, pwd2, verify_code)
            return True
        except Exception as e:
            print e.message
            return False

    def _do_login(self,usename, pwd1, pwd2, verify_code):
        pInfo = self.extract_param("pInfo", self.init_content)
        retUrl = self.extract_param("retUrl", self.init_content)
        addParam = self.extract_param("addParam", self.init_content)
        if DEBUG:
            verify_code = raw_input("请输入验证码:")
        else:
            verify_code = self.verify_code
        data = {"cbaId": usename, "cbaPw": pwd1, "captchaAnswer_enc": verify_code,
                "pInfo": pInfo, "retUrl": retUrl, "addParam": addParam}
        self.headers['Referer'] = 'https://ipin.siren24.com/i-PINM2/jsp/ipin2_j21.jsp?id=86896E62A25FB6A41157DE4C5B9DF9E1&reqNum=020201D5F6ABAE130E196F74323599D6&retUrl=23http://www.siren24.com/mysiren/member/auth/auth02p_IpinResult.jsp&ipSeq=&domSeq=7805FC8599D3C93B658CAF8D8DBC08C5&urlSeq=DB3255CAF5E621672E4306B34C86EA09&addParam=|SIR001|GCT01'
        response3 = self.request_url('https://ipin.siren24.com/i-PINM2/jsp/ipin2_j21_dup.jsp', data)
        content3 = response3.read().decode("euc-kr")

        if "자동인증 방지 문자 입력값이 올바르지 않습니다" in content3:
            raise RuntimeError('验证码错误')
        if "비밀번호 1회 오류" in content3:
            raise RuntimeError("密码或用户名出错")

        # 继续解析参数以供下次post
        data = dict()
        data["id"] = self.extract_param("id", content3)
        data["reqNum"] = self.extract_param("reqNum", content3)
        data["retUrl"] = self.extract_param("retUrl", content3)
        data["ipSeq"] = self.extract_param("ipSeq", content3)
        data["domSeq"] = self.extract_param("domSeq", content3)
        data["urlSeq"] = self.extract_param("urlSeq", content3)
        data["addParam"] = self.extract_param("addParam", content3)
        data["cbaId"] = self.extract_param("cbaId", content3, "用户名不存在")
        data["cbaPw"] = self.extract_param("cbaPw", content3)
        data["jobInfoValue"] = self.extract_param("jobInfoValue", content3)
        data["OrgCode"] = self.extract_param("OrgCode", content3)
        data["pInfo"] = self.extract_param("pInfo", content3)

        self.headers['Referer'] = 'https://ipin.siren24.com/i-PINM2/jsp/ipin2_j21_dup.jsp'
        response4 = self.request_url("https://ipin.siren24.com/i-PINM2/jsp/ipin2_j22_01.jsp", data)
        content4 = response4.read().decode("euc-kr")
        # 还要解析参数
        data = dict()
        data["IDPCODE"] = self.extract_param("IDPCODE", content4)
        data["CPCODE"] = self.extract_param("CPCODE", content4)
        data["CPREQUESTNUM"] = self.extract_param("CPREQUESTNUM", content4)
        data["RETURNURL"] = self.extract_param("RETURNURL", content4)
        data["IDPURL"] = self.extract_param("IDPURL", content4)
        data["AUTHIDINFO"] = self.extract_param("AUTHIDINFO", content4)

        self.headers['Referer'] = 'https://ipin.siren24.com/i-PINM2/jsp/ipin2_j22_01.jsp'
        response5 = self.request_url("https://cert.vno.co.kr/in.cb", data)

        self.headers['Referer'] = 'https://cert.vno.co.kr/in.cb'
        response6 = self.request_url("https://cert.vno.co.kr/IPIN/inlogin.cb")
        content6 = response6.read().decode("euc-kr")
        data = dict()
        data["CLIENTKEY"] = self.extract_param("CLIENTKEY", content6, "一级密码错误")
        data["ukey"] = self.extract_param("ukey", content6)
        data["rkey"] = self.extract_param("rkey", content6)
        data["passwd"] = pwd2  # 二次密码

        self.headers['Referer'] = 'https://cert.vno.co.kr/IPIN/inlogin.cb'
        response7 = self.request_url("https://cert.vno.co.kr/IPIN/add_auth_login_proc.cb", data)
        content7 = response7.read().decode("euc-kr")
        if "입력하신 2차비밀번호가 1회 일치하지 않습니다" in content7:
            raise RuntimeError("登陆失败，提示二次密码不正确")
        if "지금변경하기" in content7:
            # 提示更改密码,判断为登录成功
            print "登陆成功"
            print content7



glb_m = MLogin()

def init_url():
    try:
        return glb_m.init_url()
    except Exception as e:
        print e.message
        return None

def login(usename, pwd1, pwd2, verify_code):
    return glb_m.login(usename,pwd1=pwd1,pwd2=pwd2,verify_code=verify_code)


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')
    DEBUG = True
    jsessionid = init_url()
    isLogin = login(usename="wptls8208",pwd1="13565asd!",pwd2="13565asd",verify_code="")
    if isLogin:
        print "测试登陆成功."
    else:
        print "登陆失败"
