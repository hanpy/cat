#! /usr/bin/env python
# encoding=utf-8
import copy
import json
import re
import traceback
import urllib2

import pymongo

from spider.CommonSpider import CommonSpider, Dispatcher, Saver
from spider.ProxyPool import EmptyProxyPool
from spider.MQManager import RawQManager
from spider.httpreq import BasicRequests
from lxml import etree


class JyDispatcher(Dispatcher):
    def run(self):
        self.queue_manager.put_main_job({"type": "getEdition", "url": "http://www.jyeoo.com/math/ques/search"})


class JySaver(Saver):
    def __init__(self):
        Saver.__init__(self)
        self.client = pymongo.MongoClient()
        self.grade_table = self.client["jyeoo"]["grade"]

    def fail_save(self, job, **kwargs):
        pass

    def succ_save(self, res, **kwargs):
        print json.dumps(res)
        pass

    def save_grade_info(self, doc):
        self.grade_table.insert_one(doc)

    def close(self):
        self.client.close()


class JyeooSpider(CommonSpider):
    def __init__(self, worker_count):
        CommonSpider.__init__(self, spider_name="Jyeoo", worker_count=worker_count);
        self.queue_manager = RawQManager()
        self.proxy_pool = EmptyProxyPool()
        self.dispatcher = JyDispatcher(self.queue_manager)
        self.saver = JySaver()

    def check_conn(self, conn):
        if conn and conn.code == 200:
            return True
        return False

    def get_editon(self, job, breq):
        url = job["url"]
        conn = breq.request_url(url)
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
        for edition_info in edition_info_list:
            job = {"type": "getChapter", "info": edition_info}
            self.queue_manager.put_normal_job(job)
        return edition_info_list

    def get_chapter(self, job, breq):
        edition_info = job["info"]
        for grade_info in edition_info["grade"]:
            url = "http://www.jyeoo.com/math/ques/partialcategory?a=%s" % grade_info["real_id"]
            conn = breq.request_url(url)
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
            grade_info["edtion"] = copy.deepcopy(edition_info)
            grade_info["edtion"].pop("grade")
            # TODO deal with grade_info
            new_job = {"type": "getQuestionList", "grade": grade_info}
            self.queue_manager.put_normal_job(new_job)

    def run_job(self, job, **kwargs):
        breq = BasicRequests()
        breq.select_user_agent("firefox")
        if job["type"] == "getEdition":
            self.get_editon(job, breq)
        elif job["type"] == "getChapter":
            self.get_chapter(job, breq)
        elif job["type"] == "getQuestionList":
            grade_info = job["grade"]
            self.saver.save_grade_info(grade_info)
            # return self.get_question_list(job, breq)


if __name__ == "__main__":
    s = JyeooSpider(1)
    s.run()
