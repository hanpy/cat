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
from jyeoo.login import JyeooLogin
from spider.CommonSpider import CommonSpider, Dispatcher, Saver
from spider.ProxyPool import EmptyProxyPool, FileProxyPool, ADSLProxyPool
from spider.MQManager import RawQManager, RedisMqManager
from spider.httpreq import BasicRequests, SessionRequests
from lxml import etree

from spider.runtime import Log
from spider.savebin import FileSaver


class JyDispatcher(Dispatcher):
    def run(self):
        self.queue_manager.put_main_job({"type": "getEdition", "subject": "初中政治", "url": "http://www.jyeoo.com/politics/ques/search"})
#        self.queue_manager.put_main_job({"type": "getEdition", "subject": "初中数学", "url": "http://www.jyeoo.com/math/ques/search"})
#        self.queue_manager.put_main_job({"type": "getEdition", "subject": "初中物理", "url": "http://www.jyeoo.com/physics/ques/search"})
#        self.queue_manager.put_main_job({"type": "getEdition", "subject": "初中化学", "url": "http://www.jyeoo.com/chemistry/ques/search"})
#        self.queue_manager.put_main_job({"type": "getEdition", "subject": "初中生物", "url": "http://www.jyeoo.com/bio/ques/search"})
#        self.queue_manager.put_main_job({"type": "getEdition", "subject": "初中地理", "url": "http://www.jyeoo.com/geography/ques/search"})
#        self.queue_manager.put_main_job({"type": "getEdition", "subject": "初中英语", "url": "http://www.jyeoo.com/english/ques/search"})
#        self.queue_manager.put_main_job({"type": "getEdition", "subject": "初中语文", "url": "http://www.jyeoo.com/chinese/ques/search"})
#        self.queue_manager.put_main_job({"type": "getEdition", "subject": "初中历史", "url": "http://www.jyeoo.com/history/ques/search"})
#        self.queue_manager.put_main_job({"type": "getEdition", "subject": "初中科学", "url": "http://www2.jyeoo.com/science/ques/search"})
#        self.queue_manager.put_main_job({"type": "getEdition", "subject": "高中数学", "url": "http://www.jyeoo.com/math2/ques/search"})
#        self.queue_manager.put_main_job({"type": "getEdition", "subject": "高中物理", "url": "http://www.jyeoo.com/physics2/ques/search"})
#        self.queue_manager.put_main_job({"type": "getEdition", "subject": "高中化学", "url": "http://www.jyeoo.com/chemistry2/ques/search"})
#        self.queue_manager.put_main_job({"type": "getEdition", "subject": "高中生物", "url": "http://www.jyeoo.com/bio2/ques/search"})
#        self.queue_manager.put_main_job({"type": "getEdition", "subject": "高中地理", "url": "http://www.jyeoo.com/geography2/ques/search"})
#        self.queue_manager.put_main_job({"type": "getEdition", "subject": "高中英语", "url": "http://www.jyeoo.com/english2/ques/search"})
#        self.queue_manager.put_main_job({"type": "getEdition", "subject": "高中语文", "url": "http://www.jyeoo.com/chinese2/ques/search"})
#        self.queue_manager.put_main_job({"type": "getEdition", "subject": "高中政治", "url": "http://www.jyeoo.com/politics2/ques/search"})
#        self.queue_manager.put_main_job({"type": "getEdition", "subject": "高中历史", "url": "http://www.jyeoo.com/history2/ques/search"})
#        self.queue_manager.put_main_job({"type": "getEdition", "subject": "小学数学", "url": "http://www.jyeoo.com/math3/ques/search"})


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
        self.client = pymongo.MongoClient()
        self.table = self.client["jyeoo"]["question"]
        self.chapter_table = self.client["jyeoo"]["chapter"]
        self.point_table = self.client["jyeoo"]["point"]
        # self.grade_table = self.client["jyeoo"]["grade"]
        self.result_set = set()
        # self.mysql_conn = pymysql.connect(**config)
        self.fail_fsaver = FileSaver("failed.txt")
        self.question_list_saver = FileSaver("question_list.txt")
        self.init_result_set()
        self.locker = threading.RLock()

    def save_chapter(self, chapter):
        if isinstance(chapter, list):
            for item in chapter:
                self.chapter_table.insert_one(item)
        else:
            self.chapter_table.insert_one(chapter)
        print "save chapter-info success."

    def save_point(self, point):
        if isinstance(point, list):
            for item in point:
                self.point_table.insert_one(item)
        else:
            self.point_table.insert_one(point)
        print "save point-info success."

    def init_result_set(self):
        #------- mysql ----------------------------
        # with self.mysql_conn.cursor() as cursor:
        #     sql = 'SELECT url FROM question'
        #     cursor.execute(sql)
        #     for row in cursor:
        #         self.result_set.add(row["url"])
        #------- mongo ----------------------------
        return
        cursor = self.table.find()
        cnt = 0
        try:
            while True:
                doc = cursor.next()
                self.result_set.add(doc["url"])
                cnt += 1
                if cnt % 100 == 0:
                    print "add %d docs to cache." % cnt
        except StopIteration:
            print "init cache completed."

    def should_fetch(self, url):
        if url in self.result_set:
            return False
        return True

    def fail_save(self, job, **kwargs):
        self.fail_fsaver.append(json.dumps(job))

    def succ_save(self, res, **kwargs):
        print json.dumps(res)
        pass

    def save_grade_info(self, doc):
        # self.grade_table.insert_one(doc)
        pass

    def buildSet(self, question, ext_data):
        s = (ext_data["banben"], ext_data["nianjixueqi"], ext_data["zhangjie"], ext_data["tixing"],
             ext_data["nandu"], ext_data["tilei"], question["tigan"], question["xuanxiang"],
             question["tupian"], question["nandu"], question["zhenti"], question["zujuan"],
             question["kaodian"], question["fenxi"], question["jieda"], question["dianping"], question["zhentidiqu"],
             question["href"])
        return s

    def save_question(self, question, ext_data):
        #---------- mysql -----------------------
        # with self.locker:
        #     print "正在保存..."
        #     try:
        #         with self.mysql_conn.cursor() as cursor:
        #             sql = 'INSERT INTO question (banben, nianjixueqi,zhangjie,tixing,nandu,tilei,tigan,' \
        #                   'xuanxiang,tupian,nanduxishu,zhenti,zujuan,kaodian,fenxi,jieda,dianping,zhentidiqu,url)' \
        #                   ' VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        #             cursor.execute(sql, self.buildSet(question, ext_data))
        #         self.mysql_conn.commit()
        #         self.result_set.add(question["href"])
        #         print "保存成功."
        #     except Exception as e:
        #         traceback.print_exc()
        #         raise RuntimeError("存库出错。" + e.message)
        #--------- mongo ------------------------
        print "正在保存"
        try:
            doc = copy.deepcopy(question)
            doc.update(ext_data)
            doc["url"] = doc.pop("href")
            self.table.insert_one(doc)
            print "保存成功."
        except Exception as e:
            traceback.print_exc()
            raise RuntimeError("存库出错。" + e.message)

    def save_question_list(self, questions, ext_data):
        for question in questions:
            question["ext_data"] = ext_data
            self.question_list_saver.append(json.dumps(question))

    def complete(self, href, ext_data):
        cursor = self.table.find({"url":href})
        try:
            while True:
                row = cursor.next()
                if ext_data["banben"] == row["banben"] and ext_data["nianjixueqi"] == row["nianjixueqi"] and \
                    ext_data["zhangjie"] == row["zhangjie"] and ext_data["tixing"] == row["tixing"] and \
                        ext_data["nandu"] == row["nandu"] and ext_data["tilei"] == row["tilei"]:
                                return True
        except StopIteration:
            pass
        doc = copy.deepcopy(ext_data)
        doc["url"] = href
        self.table.insert_one(doc)

    def close(self):
        self.client.close()
        # self.mysql_conn.close()


class JyeooSpider(CommonSpider):
    def __init__(self, worker_count, rerun):
        CommonSpider.__init__(self, spider_name="Jyeoo", worker_count=worker_count, rerun=rerun);
        # self.queue_manager = RawQManager()
        self.queue_manager = RedisMqManager(spider_name="jyeoo")
        self.result_set = set()
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
        if not m:
            Log.error("%s:没有版本信息, 考虑只有考点信息"%job["subject"])
            return []
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

    def get_points(self, job):
        base_url = job["base_url"][:job["base_url"].rfind("/")]
        url = base_url+"/partialcategory?a=undefined&q=&f=1"
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
            chapter_info["pk"] = a.attrib.get("pk", "")
            chapter_info["name"] = a.text.strip()
            chapter_info["sessions"] = list()
            sessions = chapter.xpath("./ul/li")
            for session in sessions:
                session_info = dict()
                a = session.xpath("./a")[0]
                session_info["pk"] = a.attrib.get("pk", "")
                session_info["name"] = a.text.strip()
                session_info["points"] = list()
                points = session.xpath("./ul/li")
                for point in points:
                    point_info = dict()
                    a = point.xpath("./a")[0]
                    point_info["pk"] = a.attrib.get("pk", "")
                    point_info["name"] = a.text.strip()
                    session_info["points"].append(point_info)
                chapter_info["sessions"].append(session_info)
            chapter_info_list.append(chapter_info)
        res = dict()
        res["chapters"] = chapter_info_list
        res["subject"] = job["subject"]
        return res


    def get_chapter(self, job):
        edition_info = job["info"]
        grade_info_list = list()
        for grade_info in edition_info["grade"]:
            #if grade_info["grade_name"] not in ["七年级上", "七年级下", "八年级上", "八年级下", "九年级上", "九年级下"]:
            #    continue
            base_url = job["base_url"][:job["base_url"].rfind("/")]
            url = base_url+"/partialcategory?a=%s" % grade_info["real_id"]
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
                chapter_info["pk"] = a.attrib.get("pk", "")
                chapter_info["name"] = a.text.strip()
                chapter_info["sessions"] = list()
                sessions = chapter.xpath("./ul/li")
                for session in sessions:
                    session_info = dict()
                    a = session.xpath("./a")[0]
                    session_info["pk"] = a.attrib.get("pk", "")
                    session_info["name"] = a.text.strip()
                    session_info["points"] = list()
                    points = session.xpath("./ul/li")
                    for point in points:
                        point_info = dict()
                        a = point.xpath("./a")[0]
                        point_info["pk"] = a.attrib.get("pk", "")
                        point_info["name"] = a.text.strip()
                        session_info["points"].append(point_info)
                    chapter_info["sessions"].append(session_info)
                chapter_info_list.append(chapter_info)
            grade_info["chapters"] = chapter_info_list
            grade_info["edition"] = copy.deepcopy(edition_info)
            grade_info["edition"].pop("grade")
            grade_info["subject"] = job["subject"]
            grade_info_list.append(grade_info)
        return grade_info_list

    def get_login_session(self, acc, password):
        if getattr(self, "sreq", None):
            return self.sreq
        sreq = JyeooLogin(acc, password)
        sreq.login()
        self.sreq = sreq
        return sreq

    def request_url(self, url, **kwargs):
        sreq = getattr(self._tls, "sreq", None)
        if not sreq:
            sreq = SessionRequests()
            kwargs.update({"proxies": self.proxy_pool.get_one()})
            kwargs.update({"timeout": 6})
            sreq.request_url('http://www.jyeoo.com/', **kwargs)
            sreq.select_user_agent("firefox")
        kwargs.update({"timeout": 6})
        kwargs["proxy_credit"] = 1
        kwargs.update({"proxies": self.proxy_pool.get_one("bind")})
        conn = sreq.request_url(url, **kwargs)
        setattr(self._tls, 'sreq', sreq)
        return conn

    def run_job(self, job, **kwargs):
        if job["type"] == "getEdition":
            edition_info_list = self.get_editon(job)
            for edition_info in edition_info_list:
                job1 = {"type": "getChapter", "info": edition_info, "base_url":job["url"], "subject": job["subject"]}
                self.queue_manager.put_main_job(job1)
            job2 = {"type": "getPoint", "base_url":job["url"], "subject": job["subject"]}
            self.queue_manager.put_main_job(job2)

        elif job["type"] == "getPoint":
            point_info_list = self.get_points(job)
            self.saver.save_point(point_info_list)
            return

        elif job["type"] == "getChapter":
            grade_info_list = self.get_chapter(job)
            ##############
            # 只爬章节
            ##############
            #self.saver.save_chapter(grade_info_list)
            #return
            for grade_info in grade_info_list:
                new_job = dict()
                new_job["edition_name"], new_job["grade_id"], new_job["grade_name"], new_job["grade_real_id"] = \
                    grade_info["edition"]["name"], grade_info["grade_id"], grade_info["grade_name"], grade_info[
                        "real_id"]
                for chapter in grade_info["chapters"]:
                    for session in chapter["sessions"]:
                        for point in session["points"]:
                            t = copy.deepcopy(new_job)
                            t["point_name"], t["session_name"], t["chapter_name"] = \
                                point["name"], session["name"], chapter["name"]
                            t["pk"] = point["pk"]
                            t["type"] = "getQuestionList"
                            t["subject"] = job["subject"]
                            t["page"] = 1
                            self.queue_manager.put_main_job(t)

        elif job["type"] == "getQuestionList":
            data = dict()
            data["banben"], data["nianjixueqi"], data["zhangjie"] = \
                job["edition_name"], job["grade_name"], job["point_name"]
            config = {"pd": [1, 0],  # 1是真题，0不是
                      "ct": {1: "选择题", 2: "填空题", 9: "解答题"},
                      "dg": {1: "基础题", 2: "中档题", 3: "难题"},
                      "fg": {1: "中考题", 2: "好题", 4: "易错题", 8: "常考题", 16: "压轴题"}
                      }
            for pd in config["pd"]:
                for ct_key, ct_value in config["ct"].items():
                    for dg_key, dg_value in config["dg"].items():
                        for fg_key, fg_value in config["fg"].items():
                            url = "http://www.jyeoo.com/%s/ques/partialques?" % job["subject"]
                            data["tixing"], data["nandu"], data["tilei"] = ct_value, dg_value, fg_value
                            url += "q=%s&f=0&ct=%d&dg=%d&fg=%d&po=0&pd=%d&pi=%d&r=%s" % \
                                   (job["pk"], ct_key, dg_key, fg_key, pd, job["page"], str(random.random()))

                            new_job = dict()
                            new_job["type"] = "getQuestionDetail"
                            new_job["url"], new_job["ext_data"], new_job["page"] = url, copy.deepcopy(data), 1
                            self.queue_manager.put_normal_job(new_job)

        elif job["type"] == "getQuestionDetail":
            url = job["url"]
            url = re.sub(re.compile("(pi=\d+)", re.S), "pi=%d"%job["page"], url)
            data = job["ext_data"]
            # questions = job["questions"]
            #sreq = self.get_login_session("sssysu_hanpy@163.com", "138138")
            #p = Parser(self.proxy_pool, sreq)
            p = Parser(self.proxy_pool)
            pages = 1
            if job["page"] == 1:
                questions, pages, curr_page = p.parse(url, job=job, get_page=True)
            else:
                questions, curr_page = p.parse(url, job=job)
            if job["page"] != curr_page:
                raise RuntimeError("req_page=%d, curr_page=%d, IP被封"%(job["page"], curr_page))
            for pg in range(2, pages + 1):
                new_job = copy.deepcopy(job)
                new_job["page"] = pg
                new_job["first"] = questions[0]
                new_job["notFirstPage"] = True
                self.queue_manager.put_normal_job(new_job)
            self.saver.save_question_list(questions, data)
            Log.error("save %d questions. curr_page:%d"%(len(questions), curr_page))

            # for question in questions:
            #     href = question["href"]
            #     if self.saver.should_fetch(href):
            #         p.parse_detail(href, question, job=job)
            #         self.saver.save_question(question, ext_data=data)
            #     else:
            #         if self.saver.complete(href=href,ext_data=data):
            #             Log.info("skip:%s" % href)
            #         else:
            #             Log.info("补全%s"%href)

                    # 所有的都拿下来才保存
                    # for question in questions:
                    #     self.saver.save_question(question = question,ext_data = data)


import spider.util
from optparse import OptionParser

if __name__ == "__main__":
    spider.util.use_utf8()
    parser = OptionParser()
    parser.add_option("-r", "--rerun",
                      action="store_true", dest="is_rerun", default=False,
                      help="rerun flag")
    (options, args) = parser.parse_args()
    s = JyeooSpider(8, rerun=options.is_rerun)
    s.run()
