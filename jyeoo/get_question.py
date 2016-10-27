#! /usr/bin/env python
# encoding=utf-8
import copy
import json
import random
import re
import threading
import traceback
from jyeoo.parse import Parser
import pymongo
import pymysql

from spider.CommonSpider import CommonSpider, Dispatcher, Saver
from spider.ProxyPool import EmptyProxyPool, FileProxyPool, ADSLProxyPool
from spider.MQManager import RawQManager, RedisMqManager
from spider.httpreq import BasicRequests, SessionRequests
from lxml import etree

from spider.runtime import Log
from spider.savebin import FileSaver


class JyDispatcher(Dispatcher):
    def run(self):
        self.queue_manager.put_main_job({"type": "getEdition","subject":"math","url": "http://www.jyeoo.com/math/ques/search"})


config = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': '138138',
    'db': 'jyeoo',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
}
class JySaver(Saver):
    def __init__(self):
        Saver.__init__(self)
        # self.client = pymongo.MongoClient()
        # self.grade_table = self.client["jyeoo"]["grade"]
        self.mysql_conn = pymysql.connect(**config)
        self.fail_fsaver = FileSaver("failed.txt")
        self.locker = threading.RLock()

    def fail_save(self, job, **kwargs):
        self.fail_fsaver.append(json.dumps(job))

    def succ_save(self, res, **kwargs):
        print json.dumps(res)
        pass

    def save_grade_info(self, doc):
        self.grade_table.insert_one(doc)

    def buildSet(self,question,ext_data):
        s = (ext_data["banben"],ext_data["nianjixueqi"],ext_data["zhangjie"],ext_data["tixing"],
                ext_data["nandu"],ext_data["tilei"],question["tigan"],question["xuanxiang"],
                question["tupian"], question["nandu"],question["zhenti"],question["zujuan"],
                question["kaodian"],question["fenxi"],question["jieda"],question["dianping"],question["zhentidiqu"],question["href"])
        return s

    def save_question(self,question, ext_data):
        with self.locker:
            print "正在保存..."
            try:
                with self.mysql_conn.cursor() as cursor:
                    sql = 'INSERT INTO question (banben, nianjixueqi,zhangjie,tixing,nandu,tilei,tigan,' \
                          'xuanxiang,tupian,nanduxishu,zhenti,zujuan,kaodian,fenxi,jieda,dianping,zhentidiqu,url)' \
                          ' VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    cursor.execute(sql,self.buildSet(question,ext_data))
                self.mysql_conn.commit()
                print "保存成功."
            except Exception as e:
                traceback.print_exc()
                raise RuntimeError("存库出错。"+e.message)

    def close(self):
        # self.client.close()
        self.mysql_conn.close()


class JyeooSpider(CommonSpider):
    def __init__(self, worker_count):
        CommonSpider.__init__(self, spider_name="Jyeoo", worker_count=worker_count);
        # self.queue_manager = RawQManager()
        self.queue_manager = RedisMqManager(spider_name="jyeoo")
        self.result_set=set()
        # self.proxy_pool = FileProxyPool('static_proxy')
        self.proxy_pool = ADSLProxyPool()
        self.dispatcher = JyDispatcher(self.queue_manager)
        self.saver = JySaver()

    def check_conn(self, conn):
        if conn and conn.code == 200:
            return True
        return False

    def get_editon(self, job):
        url = job["url"]
        conn = self.request_url(url)
        if not self.check_conn(conn):
            raise RuntimeError("版本信息访问出错")
        content = conn.text.encode("utf-8")
        m = re.search('<tr class="JYE_EDITION">(.*?)</tr>', content, re.S)
        edition_info_list = list()
        for id, edition in re.findall('<a.*?data-id="(.*?)".*?>\s*(.*?)\s*</a>', m.group(1), re.S):
            edition_info = dict()
            edition_info["id"], edition_info["name"] = id, edition
            edition_info["grade"] = list()
            tr_id = "tr_" + id
            m = re.search('<tr.*?id="%s".*?>(.*?)</tr>' % tr_id, content, re.S)
            for item in re.findall('<a.*?data-id="(.*?)".*?_gradeClick\(.*?\'(.*?)\'\).*?>\s*(.*?)\s*</a>',
                                   m.group(1), re.S):
                grade_info = dict()
                grade_info["grade_id"], grade_info["real_id"], grade_info["grade_name"] = item
                edition_info["grade"].append(grade_info)
            edition_info_list.append(edition_info)
        return edition_info_list

    def get_chapter(self, job):
        edition_info = job["info"]
        grade_info_list = list()
        for grade_info in edition_info["grade"]:
            if grade_info["grade_name"]!="七年级上":
                continue
            url = "http://www.jyeoo.com/math/ques/partialcategory?a=%s" % grade_info["real_id"]
            conn = self.request_url(url)
            if not self.check_conn(conn):
                raise RuntimeError("章节信息访问出错。")
            content = conn.text.encode("utf-8")
            try:
                xmlcontent = re.sub(re.compile('(<script.*?</script>)', re.S), '', content).strip()
                xmlcontent = xmlcontent.decode("utf-8")
                root = etree.HTML(xmlcontent).xpath('./body/ul')[0]
            except Exception:
                traceback.print_exc()
                raise RuntimeError("解析章节信息出错")
            chapters = root.xpath("./li")
            chapter_info_list = list()
            for chapter in chapters:
                chapter_info = dict()
                a = chapter.xpath("./a")[0]
                chapter_info["pk"] = a.attrib["pk"]
                chapter_info["name"] = a.text.strip()
                chapter_info["sessions"] = list()
                sessions = chapter.xpath("./ul/li")
                for session in sessions:
                    session_info = dict()
                    a = session.xpath("./a")[0]
                    session_info["pk"] = a.attrib["pk"]
                    session_info["name"] = a.text.strip()
                    session_info["points"] = list()
                    points = session.xpath("./ul/li")
                    for point in points:
                        point_info = dict()
                        a = point.xpath("./a")[0]
                        point_info["pk"] = a.attrib["pk"]
                        point_info["name"] = a.text.strip()
                        session_info["points"].append(point_info)
                    chapter_info["sessions"].append(session_info)
                chapter_info_list.append(chapter_info)
            grade_info["chapters"] = chapter_info_list
            grade_info["edition"] = copy.deepcopy(edition_info)
            grade_info["edition"].pop("grade")
            grade_info_list.append(grade_info)
        return grade_info_list

    def request_url(self,url,**kwargs):
        sreq = getattr(self._tls,"sreq",None)
        if not sreq:
            sreq = SessionRequests()
            kwargs.update({"proxies": self.proxy_pool.get_one()})
            kwargs.update({"timeout": 6})
            sreq.request_url('http://www.jyeoo.com/',**kwargs)
            sreq.select_user_agent("firefox")
        kwargs.update({"timeout": 6})
        kwargs["proxy_credit"] = 1
        kwargs.update({"proxies": self.proxy_pool.get_one()})
        conn = sreq.request_url(url,**kwargs)
        setattr(self._tls,'sreq',sreq)
        return conn

    def run_job(self, job, **kwargs):
        if job["type"] == "getEdition":
            edition_info_list = self.get_editon(job)
            for edition_info in edition_info_list:
                job = {"type": "getChapter", "info": edition_info, "subject":job["subject"]}
                self.queue_manager.put_main_job(job)

        elif job["type"] == "getChapter":
            grade_info_list = self.get_chapter(job)
            for grade_info in grade_info_list:
                new_job = dict()
                new_job["edition_name"], new_job["grade_id"], new_job["grade_name"], new_job["grade_real_id"]=\
                    grade_info["edition"]["name"], grade_info["grade_id"], grade_info["grade_name"],grade_info["real_id"]
                for chapter in grade_info["chapters"]:
                    for session in chapter["sessions"]:
                        for point in session["points"]:
                            t =  copy.deepcopy(new_job)
                            t["point_name"], t["session_name"], t["chapter_name"] = \
                                point["name"], session["name"], chapter["name"]
                            t["pk"] = point["pk"]
                            t["type"]="getQuestionList"
                            t["subject"]=job["subject"]
                            t["page"]=1
                            self.queue_manager.put_main_job(t)

        elif job["type"] == "getQuestionList":
            data=dict()
            data["banben"], data["nianjixueqi"],data["zhangjie"] = \
                job["edition_name"], job["grade_name"],job["point_name"]
            config ={"pd":[0,1], #1是真题，0不是
                    "ct":{1:"选择题",2:"填空题",9:"解答题"},
                    "dg":{1:"基础题",2:"中档题",3:"难题"},
                    "fg":{1:"中考题",2:"好题", 4:"易错题", 8:"常考题", 16:"压轴题"}
                    }
            for pd in config["pd"]:
                for ct_key, ct_value in config["ct"].items():
                    for dg_key,dg_value in config["dg"].items():
                        for fg_key, fg_value in config["fg"].items():
                            url = "http://www.jyeoo.com/%s/ques/partialques?" % job["subject"]
                            data["tixing"], data["nandu"], data["tilei"] = ct_value, dg_value, fg_value
                            url+="q=%s&f=0&ct=%d&dg=%d&fg=%d&po=0&pd=%d&pi=%d&r=%s"%\
                                 (job["pk"], ct_key,dg_key,fg_key,pd,job["page"],str(random.random()))

                            new_job=dict()
                            new_job["type"]="getQuestionDetail"
                            new_job["url"], new_job["ext_data"],new_job["page"]=url,copy.deepcopy(data),1
                            self.queue_manager.put_normal_job(new_job)

        elif job["type"]=="getQuestionDetail":
            url = job["url"]
            data=job["ext_data"]
            # questions = job["questions"]
            p = Parser(self.proxy_pool)
            pages=1
            if job["page"]==1:
                questions,pages = p.parse(url,job=job,get_page=True)
            else:
                questions = p.parse(url,job=job)
            for pg in range(2,pages+1):
                new_job = copy.deepcopy(job)
                new_job["page"]=pg
                self.queue_manager.put_main_job(new_job)

            for question in questions:
                href = question["href"]
                if href not in self.result_set:
                    p.parse_detail(href, question,job=job)
                    self.result_set.add(href)
                    self.saver.save_question(question,ext_data=data)
                else:
                    Log.info("skip:%s"%href)

            # 所有的都拿下来才保存
            # for question in questions:
            #     self.saver.save_question(question = question,ext_data = data)

import spider.util
if __name__ == "__main__":
    spider.util.use_utf8()
    s = JyeooSpider(2)
    s.run()
