#! /usr/bin/env python
# encoding:utf-8

"""
@author:hanpy
@time: 16/7/29 17:15
"""
# 安装stompy模块
# 参考https://pythonhosted.org/stompy/reference/stompy.simple.html
# pip install python-qpid-proton AMQP1.0客户端
###################################################################

import random
import threading
import traceback

import proton
import redis
import time
import Queue
import json

from proton import Message
from proton.reactor import Container
from proton.utils import BlockingConnection, BlockingSender
from stompy import stomp
import threading

from stompy.frame import UnknownBrokerResponseError

from spider.httpreq import BasicRequests
from spider.runtime import Log


class RestMqManager(object):
    @staticmethod
    def clear_queue_by_jmsxgrp(qname, jmsxgrp="0", host="http://localhost:8161", broker_name="localhost"):
        qname = qname.replace("/", "!/")
        url = "%s/api/jolokia/exec/org.apache.activemq:type=Broker,brokerName=%s,destinationType=Queue," \
              "destinationName=%s/removeMatchingMessages(java.lang.String)/JMSXGroupSeq=%s" % (
                  host, broker_name, qname, jmsxgrp)
        req = BasicRequests()
        con = req.request_url(url=url, auth=("admin", "admin"))
        if not con or con.code != 200:
            print "can not exec. check mq state."
            return None
        j = json.loads(con.text.encode("utf-8"))
        return j

    @staticmethod
    def get_all(host="http://localhost:8161", broker_name="localhost"):
        url = "%s/api/jolokia/read/org.apache.activemq:type=Broker,brokerName=%s" % (host, broker_name)
        req = BasicRequests()
        con = req.request_url(url=url, auth=("admin", "admin"))
        if not con or con.code != 200:
            print "can not get the info. check mq state."
            return None
        j = json.loads(con.text.encode("utf-8"))
        return j

    @staticmethod
    def get_dest_info(dest, host="http://localhost:8161", broker_name="localhost"):
        dest = dest.replace("/", "!/")
        url = "%s/api/jolokia/read/org.apache.activemq:type=Broker,brokerName=%s,destinationName=%s,destinationType=Queue" \
              % (host, broker_name, dest)
        print url
        req = BasicRequests()
        con = req.request_url(url=url, auth=("admin", "admin"))
        if not con or con.code != 200:
            print "can not get the info."
            return None
        j = json.loads(con.text.encode("utf-8"))
        return j


class ActiveMqClient(object):
    def put(self, message, persistent=True):
        raise NotImplementedError

    def get(self, wait=False, callback=None):
        raise NotImplementedError

    def getMqInfo(self):
        mqinfo = RestMqManager.get_dest_info(dest=self.qname)
        if "value" not in mqinfo:
            raise RuntimeError("can not query queue state.")
        else:
            return mqinfo["value"]


class SingleWayStomp(object):
    def __init__(self, channel, hostname="localhost", port=61613, subscribe=True, ack="client"):
        self._channel = channel
        self._stomp = stomp.Stomp(hostname=hostname, port=port)
        self._stomp.connect(clientid=self.__hash__())
        self.ack = ack
        if subscribe:
            self._stomp.subscribe({"destination": self._channel, "ack": ack})  # 有时候不能即时订阅成功,拿消息需要等待
            self._subscribe = True
        else:
            self._subscribe = False
        self._locker = threading.RLock()
        self._plocker = threading.RLock()

    def put(self, message, persistent=True):
        with self._plocker:
            msg = json.dumps(message, ensure_ascii=True)
            tx = {'transaction': self.__hash__()}
            self._stomp.begin(tx)
            if persistent:
                self._stomp.send({"destination": self._channel, "body": msg, "persistent": "true"})
            else:
                self._stomp.send({"destination": self._channel, "body": msg, "persistent": "false"})
            self._stomp.commit(tx)

    def get_nowait(self):
        if self._subscribe:
            with self._locker:
                m = self._stomp.receive_frame(nonblocking=False)
                if self.ack == "client":
                    self._stomp.ack(frame=m)
                return json.loads(m.body)
        else:
            raise RuntimeError("this connection did not subscribe channel")

    def get(self, callback=None):
        if self._subscribe:
            with self._locker:
                m = self._stomp.receive_frame(callback=callback, nonblocking=False)
                self._stomp.ack(frame=m)
                return json.loads(m.body)
        else:
            raise RuntimeError("this connection did not subscribe channel")

    def disconnect(self):
        self._stomp.disconnect()


class STOMPClient(ActiveMqClient):
    def __init__(self, channel, hostname="localhost", port=61613):
        ActiveMqClient.__init__(self)
        self._channel = channel
        self._hostname = hostname
        self.qname = channel
        self._port = port
        self._putter = SingleWayStomp(channel=channel, hostname=hostname, port=port, subscribe=False)
        self._getter = SingleWayStomp(channel=channel, hostname=hostname, port=port, subscribe=True, ack="client")

    def put(self, message, persistent=True):
        return self._putter.put(message=message, persistent=persistent)

    def get(self, wait=False, callback=None):
        if not wait:
            return self._getter.get_nowait()
        else:
            return self._getter.get(callback=callback)

    def clear(self):
        i = 0
        while True:
            try:
                job = self._getter.get_nowait()
                if not job:
                    Log.error("clean complete.")
                    break
                else:
                    i += 1
                    if (i > 100):
                        print ".",
                        i = 1
            except UnknownBrokerResponseError as e:
                if e.message == "empty reply":
                    Log.error("clean complete")
                    break
                else:
                    print ".",
                    time.sleep(1)


class RBlockingConnection(BlockingConnection):
    def __init__(self, url, timeout=None, container=None, ssl_domain=None, heartbeat=None, **kwargs):
        BlockingConnection.__init__(self, url=url, timeout=timeout, container=container, ssl_domain=ssl_domain,
                                    heartbeat=heartbeat, **kwargs)
        self.conn = self.container.connect(url=self.url, handler=self, ssl_domain=ssl_domain, reconnect=None,
                                           heartbeat=heartbeat, **kwargs)
        self.wait(lambda: not (self.conn.state & proton.Endpoint.REMOTE_UNINIT),
                  msg="Opening connection")


#################################
# AMQP协议客户端,会超时
# credit=prefechsize,预取消息个数,预取的消息不会发给别的队列,设为0或1会导致无法继续获取.Bug?
# ToDO 超时策略?
#################################
class AMQPClient(ActiveMqClient):
    def __init__(self, address, host="localhost:5672"):
        ActiveMqClient.__init__(self)
        self.address = address
        self.qname = address
        self.host = host
        # self.container = Container()
        # self.conn1 = self.container.connect(url=self.host, handler=self, reconnect=True, heartbeat=1000)
        self.conn1 = BlockingConnection(host, timeout=60, heartbeat=3000)
        self.conn2 = BlockingConnection(host, timeout=60, heartbeat=3000)
        self.sender = self.conn1.create_sender(address=address)
        self.receiver = self.conn2.create_receiver(address=address, credit=4)
        self.putlocker = threading.RLock()
        self.getlocker = threading.RLock()
        self.getTime = time.time()

    def reset_sender(self):
        while True:
            try:
                try:
                    self.conn1.close()
                except Exception as e:
                    print e.message
                    pass
                self.conn1 = BlockingConnection(self.host, timeout=600, heartbeat=3000)
                self.sender = self.conn1.create_sender(address=self.address)
                return
            except Exception as e:
                Log.error(e.message)
                time.sleep(10)

    def reset_receiver(self):
        while True:
            try:
                try:
                    self.conn2.close()
                except:
                    pass
                self.conn2 = BlockingConnection(self.host, timeout=60, heartbeat=3000)
                self.receiver = self.conn2.create_receiver(address=self.address, credit=4)
                return
            except Exception as e:
                Log.error(e.message)
                time.sleep(10)

    def put(self, message, persistent=True):
        with self.putlocker:
            message = json.dumps(message)
            while True:
                try:
                    self.sender.send(Message(body=message, durable=persistent), timeout=5)
                    return
                except Exception as e:
                    Log.error(e.message)
                    self.reset_sender()

    def get(self, wait=0.1, callback=None):
        # self.reset_receiver()
        with self.getlocker:
            # will throw TimeOut
            curr = time.time()
            if curr - self.getTime > 30:
                self.reset_receiver()
            self.getTime = curr
            try:
                msg = self.receiver.receive(timeout=wait)
                self.receiver.accept()
            except proton.Timeout:
                return None
            except proton.utils.ConnectionException:
                # 连接60秒超时,超时需要重连
                self.reset_receiver()
                return None
        if callback:
            callback(msg)
        else:
            return json.loads(msg.body)

    def clear(self):
        j = RestMqManager.clear_queue_by_jmsxgrp(qname=self.address)
        Log.error("%s clean complete. %d messages deleted." % (self.address, j["value"]))

    def get_noaccept(self, wait=0.1, callback=None):
        with self.getlocker:
            # will throw TimeOut
            try:
                msg = self.receiver.receive(timeout=wait)
                self.receiver.release()
            except proton.Timeout:
                return None
        if callback:
            callback(msg)
        else:
            return json.loads(msg.body)

    def close(self):
        try:
            self.receiver.close()
        except Exception:
            pass
        try:
            self.sender.close()
        except Exception:
            pass
        try:
            self.conn1.close()
        except Exception:
            pass
        try:
            self.conn2.close()
        except Exception:
            pass

    def __del__(self):
        self.close()


class CommonQueueManager(object):
    def get_main_job(self):
        raise NotImplementedError()

    def get_normal_job(self):
        raise NotImplementedError()

    def get_retry_job(self):
        raise NotImplementedError()

    def put_main_job(self, job, **kwargs):
        raise NotImplementedError()

    def put_normal_job(self, job, **kwargs):
        raise NotImplementedError()

    def put_retry_job(self, job, fail_handler=None, retry_limit=10, **kwargs):
        raise NotImplementedError()

    def get_job(self, **kwargs):
        raise NotImplementedError()

    def close(self, **kwargs):
        raise NotImplementedError()

    def empty(self):
        raise NotImplementedError()

    def get_real_qsize(self, qtype):
        raise NotImplementedError()

    def clean_main_queue(self):
        raise  NotImplementedError()

    def stop_getting(self):
        raise NotImplementedError()

class RawQManager(CommonQueueManager):
    def __init__(self):
        self.main_q = Queue.Queue()
        self.normal_q = Queue.Queue()
        self.retry_q = Queue.Queue()
        self.get_pause=False

    def get_retry_job(self):
        if self.get_pause:
            Log.error("wait to get retry job.")
        while self.get_pause:
            time.sleep(1)
        try:
            ret = self.retry_q.get_nowait()
            return ret
        except Queue.Empty:
            return None

    def get_job(self, **kwargs):
        deep = kwargs.get("deep", 1)
        if deep > 10:
            return None
        try:
            job = self.get_normal_job()
            if job: return job
            if random.randint(0, 100) < 30:
                job = self.get_retry_job()
            if job: return job
            job = self.get_main_job() or self.get_retry_job()
            if job: return job
        except Exception as e:
            Log.error(e.message)
            traceback.print_exc()
        time.sleep(0.1)
        return self.get_job(deep=deep + 1)

    def stop_getting(self):
        self.get_pause = True

    def get_normal_job(self):
        if self.get_pause:
            Log.error("wait to get normal job.")
        while self.get_pause:
            time.sleep(1)
        try:
            ret = self.normal_q.get_nowait()
            return ret
        except Queue.Empty:
            return None

    def empty(self):
        if self.main_q.empty() and self.normal_q.empty() and self.retry_q.empty():
            return True
        else:
            return False

    def close(self, **kwargs):
        Log.error("%d main jobs, %d normal jobs, %d retry jobs left in queue."
                  %(self.main_q.qsize(), self.normal_q.qsize(), self.retry_q.qsize()))

    def get_real_qsize(self, qtype):
        return eval("self.%s_q"%qtype).qsize()

    def put_main_job(self, job, **kwargs):
        limit = kwargs.get("limit", None)
        if limit:
            while self.get_real_qsize(qtype="main") >= limit:
                time.sleep(1)
        wait=kwargs.get("wait",None)
        try:
            if not wait:
                self.main_q.put_nowait(job)
            else:
                self.main_q.put(block=True,timeout=wait)
        except Queue.Full as e:
            Log.error("push job failed.Error:%s. Job is %s" % (e.message, json.dumps(job)))

    def clean_main_queue(self):
        while not self.main_q.empty():
            self.main_q.get_nowait()

    def put_retry_job(self, job, fail_handler=None, retry_limit=10, **kwargs):
        limit = kwargs.get("limit", None)
        if limit:
            while self.get_real_qsize(qtype="retry") >= limit:
                time.sleep(1)
        wait = kwargs.get("wait", None)
        try:
            if not wait:
                self.retry_q.put_nowait(job)
            else:
                self.retry_q.put(block=True, timeout=wait)
        except Queue.Full as e:
            Log.error("push job failed.Error:%s. Job is %s" % (e.message, json.dumps(job)))

    def put_normal_job(self, job, **kwargs):
        limit = kwargs.get("limit", None)
        if limit:
            while self.get_real_qsize(qtype="normal") >= limit:
                time.sleep(1)
        wait = kwargs.get("wait", None)
        try:
            if not wait:
                self.normal_q.put_nowait(job)
            else:
                self.normal_q.put(block=True, timeout=wait)
        except Queue.Full as e:
            Log.error("push job failed.Error:%s. Job is %s" % (e.message, json.dumps(job)))

    def get_main_job(self):
        if self.get_pause:
            Log.error("wait to get main job.")
        while self.get_pause:
            time.sleep(1)
        try:
            ret = self.main_q.get_nowait()
            return ret
        except Queue.Empty:
            return None


class AmqpMqManager(CommonQueueManager):
    def __init__(self, spider_name="spider"):
        CommonQueueManager.__init__(self)
        self.spider_name = spider_name
        self.main_q = AMQPClient(address=spider_name + ".main")
        self.normal_q = AMQPClient(address=spider_name + ".normal")
        self.retry_q = AMQPClient(address=spider_name + ".retry")
        self.init_queue_info()
        self.end_flag = False
        self.get_pause = False
        self.locker = threading.RLock()

    def close(self):
        self.end_flag = True
        self.main_q.close()
        self.normal_q.close()
        self.retry_q.close()

    def stop_getting(self):
        self.get_pause=True

    def empty(self):
        mainq_size = self.get_real_qsize(qtype="main")
        retryq_size = self.get_real_qsize(qtype="retry")
        normalq_size = self.get_real_qsize(qtype="normal")
        return mainq_size == 0 and retryq_size == 0 and normalq_size == 0

    def init_queue_info(self):
        self.mainq_size = self.get_real_qsize("main")
        self.normalq_size = self.get_real_qsize("normal")
        self.retryq_size = self.get_real_qsize("retry")
        Log.info(
            "mainq_size=%d,normalq_size=%d,retryq_size=%d" % (self.mainq_size, self.normalq_size, self.retryq_size))

    def get_queue_info(self, qtype):
        qinfo = eval("self.%s_q" % qtype).getMqInfo()
        return qinfo

    def get_real_qsize(self, qtype):
        qsize = self.get_queue_info(qtype)["QueueSize"]
        return qsize

    def get_job(self, **kwargs):
        deep = kwargs.get("deep", 1)
        if deep > 10:
            return None
        try:
            job = self.get_normal_job()
            if job: return job
            if random.randint(0, 100) < 30:
                job = self.get_retry_job()
            if job: return job
            job = self.get_main_job() or self.get_retry_job()
            if job: return job
        except Exception as e:
            Log.error(e.message)
            traceback.print_exc()
        time.sleep(1)
        return self.get_job(deep=deep + 1)

    def put_main_job(self, job, **kwargs):
        mainq_limit = kwargs.get("limit", None) or getattr(self, "mainq_limit", None)
        if mainq_limit:
            while self.mainq_size >= mainq_limit and not self.end_flag:
                time.sleep(1)
        self.main_q.put(message=job)
        with self.locker:
            self.mainq_size += 1

    def put_normal_job(self, job, **kwargs):
        normalq_limit = kwargs.get("limit", None) or getattr(self, "normalq_limit", None)
        if normalq_limit:
            while self.normalq_limit >= normalq_limit and not self.end_flag:
                time.sleep(1)
        self.normal_q.put(message=job)
        with self.locker:
            self.normalq_size += 1

    def put_retry_job(self, job, fail_handler=None, retry_limit=10, **kwargs):
        retryq_limit = kwargs.get("limit", None) or getattr(self, "retryq_limit", None)
        if retryq_limit:
            while self.retryq_size >= retryq_limit:
                time.sleep(1)
        if not isinstance(job, dict):
            raise RuntimeError("Job should be a dict.")
        failcnt = job.get("_failcnt_", 0)
        job["_failcnt_"] = failcnt + 1
        if failcnt > retry_limit:
            if not fail_handler:
                raise RuntimeError("No handler for failed job.")
            else:
                fail_handler(job)
                return
        self.retry_q.put(message=job)
        with self.locker:
            self.retryq_size += 1

    def get_main_job(self):
        if self.get_pause:
            Log.error("wait to get main job.")
        while self.get_pause:
            time.sleep(1)
        job = self.main_q.get()
        if job:
            with self.locker:
                self.mainq_size -= 1
        return job

    def get_normal_job(self):
        if self.get_pause:
            Log.error("wait to get noraml job.")
        while self.get_pause:
            time.sleep(1)
        job = self.normal_q.get()
        if job:
            with self.locker:
                self.normalq_size -= 1
        return job

    def get_retry_job(self):
        if self.get_pause:
            Log.error("wait to get retry job.")
        while self.get_pause:
            time.sleep(1)

        job = self.retry_q.get()
        if job:
            with self.locker:
                self.retryq_size -= 1
        return job

    def clean_main_queue(self):
        self.main_q.clear()
        with self.locker:
            self.mainq_size = 0


class RedisMqManager(CommonQueueManager):
    def __init__(self, spider_name="spider", **kwargs):
        if kwargs is None:
            self.redis = redis.Redis()
        else:
            self.redis = redis.Redis(**kwargs)
        self.name = spider_name
        self.main_qname = self.name + ".main"
        self.retry_qname = self.name + ".retry"
        self.normal_qname = self.name + ".normal"
        self.main_qsize = 0
        self.retry_qsize = 0
        self.normal_qsize = 0
        self.init_queueu_info()
        self.get_pause = False

    def stop_getting(self):
        self.get_pause= True

    def get_real_qsize(self, qtype):
        qname = eval("self.%s_qname"%qtype)
        return self.redis.llen(qname)

    def init_queueu_info(self):
        self.main_qsize = self.redis.llen(self.main_qname)
        self.retry_qsize = self.redis.llen(self.main_qname)
        self.normal_qsize = self.redis.llen(self.normal_qname)

    def get_normal_job(self):
        if self.get_pause:
            Log.error("wait to get normal job.")
        while self.get_pause:
            time.sleep(1)
        ret = self.redis.blpop(self.normal_qname, timeout=1)
        if ret:
            return json.loads(ret[1])
        return None

    def put_retry_job(self, job, fail_handler=None, retry_limit=10, **kwargs):
        failcnt = job.get("_failcnt_", 0)
        failcnt += 1
        job["_failcnt_"] = failcnt
        if failcnt >= retry_limit:
            fail_handler(job)
        else:
            try:
                self.retry_qsize = self.redis.rpush(self.retry_qname, json.dumps(job))
            except Exception as e:
                Log.error("push job failed.Error:%s. Job is %s" % (e.message, json.dumps(job)))

    def get_retry_job(self):
        if self.get_pause:
            Log.error("wait to get retry job.")
        while self.get_pause:
            time.sleep(1)

        ret = self.redis.blpop(self.retry_qname, timeout=1)
        if ret:
            return json.loads(ret[1])
        return None

    def close(self, **kwargs):
        self.redis.save()

    def empty(self):
        if self.redis.llen(self.main_qname) == 0 \
                and self.redis.llen(self.retry_qname) == 0 \
                and self.redis.llen(self.normal_qname) == 0:
            return True
        else:
            return False

    def put_main_job(self, job, **kwargs):
        limit = kwargs.get("limit",None)
        if limit:
            while self.get_real_qsize(qtype="main")>=limit:
                time.sleep(1)
        try:
            self.main_qsize = self.redis.rpush(self.main_qname, json.dumps(job))
        except Exception as e:
            Log.error("push job failed.Error:%s. Job is %s" % (e.message, json.dumps(job)))

    def get_job(self, **kwargs):
        deep = kwargs.get("deep", 1)
        if deep > 10:
            return None
        try:
            job = self.get_normal_job()
            if job: return job
            if random.randint(0, 100) < 30:
                job = self.get_retry_job()
            if job: return job
            job = self.get_main_job() or self.get_retry_job()
            if job: return job
        except Exception as e:
            Log.error(e.message)
            traceback.print_exc()
        time.sleep(1)
        return self.get_job(deep=deep + 1)

    def get_main_job(self):
        if self.get_pause:
            Log.error("wait to get main job.")
        while self.get_pause:
            time.sleep(1)
        ret = self.redis.blpop(self.main_qname, timeout=1)
        if ret:
            return json.loads(ret[1])
        return None

    def put_normal_job(self, job, **kwargs):
        try:
            self.normal_qsize = self.redis.rpush(self.normal_qname, json.dumps(job))
        except Exception as e:
            Log.error("push job failed.Error:%s. Job is %s" % (e.message, json.dumps(job)))

    def clean_main_queue(self):
        self.redis.delete(self.main_qname)


# c = AMQPClient(address="test")

def timeTest(target, **args):
    curr = time.time()
    target(**args)
    print "time used:%f" % (time.time() - curr)


def mutlitest(func, thread_cnt, count):
    threads = list()
    for i in range(thread_cnt):
        t = threading.Thread(target=func, args=(count,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()


def amqpput(count):
    msg = "1" * 1000
    c = AMQPClient(address="test")
    for i in range(count):
        c.put(message=i)


def amqpget(count):
    c = AMQPClient(address="test")
    for i in range(count):
        print c.get(wait=5)


if __name__ == "__main__":
    for i in range(100):
        t = threading.Thread(target=amqpget, args=(500,))
        t.start()
    for i in range(50):
        t = threading.Thread(target=amqpput, args=(1000,))
        t.start()
