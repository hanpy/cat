#! /usr/bin/env python
# encoding:utf-8

"""
@author:hanpy
@time: 16/9/9 18:08
"""
import code
import os
import time
import traceback
import json

import signal

from spider.MQManager import CommonQueueManager, AmqpMqManager, RedisMqManager
from spider.ProxyPool import ProxyPool, ADSLProxyPool
import threading

from spider.runtime import Log, Color
from spider.savebin import FileSaver, BinSaver


class Saver(object):
    def succ_save(self, res, **kwargs):
        raise NotImplementedError()

    def fail_save(self, job, **kwargs):
        raise NotImplementedError()

    def close(self):
        pass


class Dispatcher(threading.Thread):
    def __init__(self, queue_manager):
        threading.Thread.__init__(self)
        self.queue_manager = queue_manager

    def run(self):
        raise NotImplementedError()


class CommonSpider(object):
    class GetNoneJobError(Exception): pass
    class BadJobError(Exception):pass
    class FatalError(Exception):pass
    # class Saver(Saver): pass
    # class Dispatcher(Dispatcher): pass
    def __init__(self, spider_name, worker_count=10, listen_port=28888,rerun=False):
        self.queue_manager = CommonQueueManager()
        self.proxy_pool = ProxyPool()
        self.saver = Saver()
        self.dispatcher = Dispatcher(self.queue_manager)
        self.name = spider_name
        self.listen_port = listen_port
        self.workers = list()
        self.worker_status = dict()
        self.worker_count = worker_count
        self.worker_running = 0
        self.locker = threading.RLock()
        self.end_flag = False
        self.pause_flag = False
        self._tls = threading.local()
        self.job_cache=dict()
        self.rerun_flag = rerun

    def reg_sign_handler(self):
        signal.signal(signal.SIGINT, self.sigint_handler)
        signal.signal(signal.SIGTSTP, self.sigstp_handler)
        # print "register the handler."

    def sigint_handler(self, signum, frame):
        Log.error("Receive SIGINT signal.")
        self.exit()

    def sigstp_handler(self, signum, frame):
        d = {'_frame': frame}  # Allow access to frame object.
        d.update(frame.f_globals)  # Unless shadowed by global
        d.update(frame.f_locals)
        i = code.InteractiveConsole(d)
        message = "Signal received : entering python shell."
        self.proxy_pool.close()
        self.pause()
        i.interact(message)
        if not self.end_flag:
            self.proxy_pool.run()
            self.resume()

    def run(self):
        self.listener = threading.Thread(target=self.listen)
        self.listener.start()
        self.reporter = threading.Thread(target=self.report)
        self.reporter.start()
        if not self.rerun_flag:
            self.dispatcher.start()
        self.proxy_pool.run()
        self.reg_sign_handler()

        time.sleep(2)

        for i in range(self.worker_count):
            worker = threading.Thread(target=self.init_worker, args=(i,))
            self.workers.append(worker)
            worker.start()
        while not self.end_flag:
            time.sleep(3)

    def listen(self, **kwargs):
        pass

    def report(self, **kwargs):
        pass

    def init_worker(self, worker_id, **kwargs):
        setattr(self._tls, "tid", worker_id)
        try:
            self.worker_status[worker_id] = "running"
            with self.locker:
                self.worker_running += 1
            self._to_work()
        except Exception as e:
            Log.error(e.message)
            traceback.print_exc()
        self.drop_worker(worker_id)

    def get_tid(self):
        return getattr(self._tls,"tid",-1)

    def pause(self):
        self.pause_flag=True

    def resume(self):
        self.pause_flag=False

    def stop(self):
        self.end_flag=True

    def _to_work(self):
        while not self.end_flag:
            if self.pause_flag:
                Log.error("worker%d paused." % self.get_tid())
                while self.pause_flag:
                    time.sleep(1)
                    if self.end_flag:
                        return

            job = self.queue_manager.get_job()
            self.job_cache[self.get_tid()]=job
            try:
                if not job:
                    i = 0
                    while self.queue_manager.empty():
                        if i>5:
                        # if i > 1:
                            raise CommonSpider.GetNoneJobError()
                        time.sleep(0.5)
                        i+=1
                        time.sleep(2)
                else:
                    res = self.run_job(job)
                    if res:
                        self.saver.succ_save(res=res,job=job)
            except CommonSpider.GetNoneJobError:
                Log.error("worker%d get None job."%self.get_tid())
                return
            except CommonSpider.FatalError as e:
                Log.error(e.message)
                traceback.print_exc()
                if self.deal_failjob(job):
                    self.queue_manager.put_retry_job(job, self.saver.fail_save)
                return
            except CommonSpider.BadJobError as e:
                Log.error(e.message)
                self.saver.fail_save(job=job)
            except Exception as e:
                Log.error(e.message)
                traceback.print_exc()
                if self.deal_failjob(job):
                    self.queue_manager.put_retry_job(job,fail_handler=self.saver.fail_save)
            finally:
                self.job_cache.pop(self.get_tid())

    def deal_failjob(self, job):
        fail_cnt = job.get("_failcnt_", 0) + 1
        if fail_cnt>1000:
            self.saver.fail_save(job=job)
            return False
        else:
            job["_failcnt_"]=fail_cnt
            time.sleep(1)
            return True

    def run_job(self,job,**kwargs):
        return NotImplementedError()

    def drop_worker(self, worker_id, **kwargs):
        self.worker_status[worker_id] = "stop"
        with self.locker: self.worker_running -= 1
        if self.worker_running==0:
            self.exit()

    def exit(self, **kwargs):
        self.queue_manager.stop_getting()
        if len(self.job_cache)>0:
            Log.error("There are %d jobs in job_cache."%len(self.job_cache))
            for item in self.job_cache.items():
                tid, job = item[0],item[1]
                Log.error("save thread%d's cache job."%tid)
                self.queue_manager.put_normal_job(job)
        self.proxy_pool.close()
        self.saver.close()
        self.queue_manager.close()
        Log.error("Process Done.")
        pid = os.getpid()
        os.kill(pid, signal.SIGKILL)


class TSpider(CommonSpider):
    class TDispatcher(Dispatcher):
        def __init__(self,queue_manager):
            Dispatcher.__init__(self, queue_manager=queue_manager)
        def run(self):
            for i in range(100):
                self.queue_manager.put_main_job({"id":i})
                time.sleep(0.1)
    class TSaver(Saver):
        def fail_save(self, job, **kwargs):
            print job

        def succ_save(self, res, **kwargs):
            print res

    def __init__(self):
        CommonSpider.__init__(self, spider_name="test",worker_count=3)
        self.queue_manager = AmqpMqManager(spider_name=self.name)
        self.dispatcher = TSpider.TDispatcher(self.queue_manager)
        self.saver = TSpider.TSaver()

    def run_job(self,job,**kwargs):
        return job["id"]



if __name__ == "__main__":
    t = TSpider()
    t.run()
