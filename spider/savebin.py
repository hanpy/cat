#!/usr/bin/env python
import StringIO
import gzip
import os
import struct
import threading
import csv
import cutil


# from os.path import join, getsize

class FileSaver:
    def __init__(self, fn):
        self.fd = open(fn, 'a+b')
        self.rwlock = threading.Lock()

    def __del__(self):
        self.fd.close()

    def append(self, value):
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        with self.rwlock:
            self.fd.write(value + "\n")
            self.fd.flush()


class CsvSaver:
    def __init__(self, fn, head):
        self.writer = csv.writer(open(fn, 'wb'))
        self.writer.writerow(head)
        self.lock = threading.Lock()

    def __del__(self):
        if hasattr(self.writer, 'close'):
            self.writer.close()

    def writerline(self, line):
        if isinstance(line, unicode):
            value = line.encode('utf-8')
        with self.lock:
            print "wirte csv:%s" % line
            self.writer.writerow(line)


class BinSaver:
    @staticmethod
    def compress_item(name, value):
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        if isinstance(name, unicode):
            name = name.encode('utf-8')
        fo = StringIO.StringIO()
        f = gzip.GzipFile(fileobj=fo, mode='wb')
        f.write(value)
        f.close()
        r = fo.getvalue()
        fo.close()
        return struct.pack("I", len(name)) + name + struct.pack("I", len(r)) + r

    def __init__(self, fn):
        self._fn = fn

    def append(self, name, value):
        a = BinSaver.compress_item(name, value)
        pos = cutil.mp_append_log(self._fn, a)
        if pos < 0:
            raise IOError("unable to write bin file.")
        return pos

    def filename(self):
        return self._fn

    def getsize(self):
        filename = os.path.abspath(self.filename())
        size = os.path.getsize(filename)
        return size


class BinReader:
    def __init__(self, fn):
        self._fsz = float(os.path.getsize(fn))
        self._nread = 0
        self.fn = fn
        self.fd = open(fn, 'rb')
        self.lock = threading.Lock()

    def __del__(self):
        self.fd.close()

    def _readone_i(self):
        sz0 = self.fd.read(4)
        if len(sz0) == 0:
            return (None, None)
        if len(sz0) != 4:
            raise IOError('invalid file')
        (sz,) = struct.unpack("I", sz0)
        fn = self.fd.read(sz)
        if len(fn) != sz:
            raise IOError('invalid file')
        self._nread += sz + 4

        sz0 = self.fd.read(4)
        if len(sz0) != 4:
            raise IOError('invalid file')
        (sz,) = struct.unpack("I", sz0)
        gzconn = self.fd.read(sz)
        if len(gzconn) != sz:
            raise IOError('invalid file')
        self._nread += sz + 4

        fin = StringIO.StringIO(gzconn)
        with gzip.GzipFile(fileobj=fin, mode='rb') as f:
            conn = f.read()
        fin.close()
        return (fn, conn)

    def progress(self):
        if self._fsz == 0.0:
            return 1.0
        return float(self._nread) / self._fsz

    def readone(self):
        with self.lock:
            return self._readone_i()

    def readone_at(self, pos):
        with self.lock:
            self.fd.seek(pos)
            return self._readone_i()


import pymongo
def read():
    t = BinReader('/data/crawler/jd/jd_yjs/2016/jd_yjs_201607.bin')

    count = 0
    while True:
        (a, b) = t.readone()
        if a is None:
            break

        print a
        continue

        a = a.replace(r'jd_yjsqz_qz.', '')
        if r'http' in a:
            a= a.replace(r'http://my.yingjiesheng.com/', '')
        print a
        a = a[:a.index(r'.')]
        #f = open('html/'+a+'.html', 'w')
        #f.write(b)
        #f.close()


def chg_bin(channel):
    reader = BinReader('/data/crawler/jd/%s/2016/%s_201608.bin' % (channel, channel))
    writer = BinSaver('/data/crawler/jd/%s/2016/%s_201608.bin.bak' % (channel, channel))
    while True:
        (a,b) = reader.readone()
        if not a:
            break

        x =  a.split('.')
        lst=[]
        lst.append(x[0])
        lst.append(x[1: len(x)-1])
        lst.append(x[len(x)-1])
        old_id = lst[0]+"://"+'.'.join(lst[1])


        #lst[1] = id_

        #name = '.'.join(lst)
        name = 'jd_dj_sx://'+x[1]+'.'+x[3]
        if 'click' in name:
            dellst.append(old_id)
            continue

        pos = writer.append(name, b)

        print old_id, '  ->  ',  "jd_dj_sx://"+x[1], pos


def chg_mongo(channel):
    uri = 'mongodb://hadoop2:27017/'
    mongo_client = pymongo.MongoClient(uri)
    db_ = 'jd_crawler'
    channel = "jd_dj_sx_other"
    coll_ = 'page_store_%s' % channel
    coll = mongo_client[db_][coll_]
    reader = BinReader('/data/crawler/jd/%s/2016/%s_201608.bin.bak' % (channel, channel))
    idlst = []
    binf = 'binf::/data/crawler/jd/%s/2016/%s_201608.bin::' % (channel, 'jd_dj_sx')
    while True:
        (a,b) = reader.readone()
        if not a:
            break
        x =  a.split('.')
        idlst.append(x)
    for x in idlst:
        print binf+str(x[1])
        coll.update_one({'indexUrl' : x[0]}, {'$set': {'indexUrl' : x[0], 'pageContentPath' :  binf+str(x[1])}})




def change_id():
    channel = 'jd_dj_sx_other'
    #chg_bin(channel)
    chg_mongo(channel)


if __name__ == "__main__":
    t = BinReader('/data/crawler/_files3_/project_archdaily/2016/project_archdaily_201609.bin')
    count = 0
    while True:
        (a, b) = t.readone()
        if a is None:
            break
        x = a.split('.')[1]
        x = x.replace('/', '.')
        with open('archilovers/'+x, 'w') as f:
            f.write(b) 
        count += 1
        if count % 10000 == 0:
            # print "count==%d,a==[%s],b==[%s]"%(count,a,b)
            print "count==%d," % (count)
    print "the last count=[%d]" % count
    # print "count==%d"%count
