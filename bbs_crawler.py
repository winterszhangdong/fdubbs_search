#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import requests
import time
import utils
import Queue
import threading
import pdb
from bs4 import BeautifulSoup


reload(sys)
sys.setdefaultencoding('utf-8')

headers = {'User-Agent':
           'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.72 Safari/537.36'}
insert_error = '%s: %s fail to insert into mysql! -->'
post_url = 'http://bbs.fudan.edu.cn/bbs/tdoc'


# 从数据库中获取板块信息列表
def get_blist():
    query = "SELECT * FROM board"
    s = utils.Sql()
    result = s.searchall(query)

    b_list = []
    for b in result:
        brd = {}
        brd['bid'] = b[0]
        brd['btitle'] = b[1]
        brd['bname'] = b[2]
        brd['total'] = b[3]
        b_list.append(brd)

    return b_list


# 此次爬取开始和结束的位置
def get_distance(bid, old_total):
    old_total = int(old_total)
    payload = {'bid': str(bid)}
    p = utils.Proxy()
    while True:
        try:
            r = requests.get(post_url, params=payload,
                             headers=headers, proxies=p.get_proxy(), timeout=3)
            assert r.status_code == requests.codes.ok
            soup = BeautifulSoup(r.text)
            brd = soup.find('brd')
            new_total = int(brd['total'])
            assert new_total
            break
        except Exception, e:
            print 'get total failure! -->', e
            continue
    # old_total为开始位置（即之前已经爬取过的帖子不再爬取），new_total为结束位置
    distance = (old_total, new_total)
    return distance


def get_posts(bid, start):
    payload = {'bid': bid, 'start': str(start)}
    posts = []
    while True:
        try:
            r = requests.get(post_url, params=payload,
                             headers=headers)
            assert r.status_code == requests.codes.ok
            content = r.text
            soup = BeautifulSoup(content)
            po_list = soup.find_all('po')
            assert po_list
            for po in po_list:
                post = {}
                # 获取帖子的id
                post['pid'] = po['id']
                # 获取贴子的标题
                post['title'] = po.string
                # 将帖子的信息装入post列表
                posts.append(post)
                print start, post['title']
            time.sleep(0.5)
            break
        except Exception, e:
            # print 'get post lists failure! -->', e
            continue

    return posts


# 对每一个板块多线程爬取帖子标题
class PostThread(threading.Thread):
    post_list = []
    def __init__(self, bid, queue):
        threading.Thread.__init__(self)
        self.bid = bid
        self.queue = queue

    def run(self):
        while True:
            if not self.queue.empty():
                start = self.queue.get()
                posts = get_posts(self.bid, start)
                PostThread.post_list.extend(posts)
                self.queue.task_done()


def save_posts(brd, post_list):
    query = "REPLACE INTO post (pid, ptitle, bid, bname, btitle) VALUES (%s, %s, %s, %s, %s)"
    s = utils.Sql()
    param_list = []
    for post in post_list:
        param = (
            int(post['pid']), post['title'], int(brd['bid']),
            brd['bname'], brd['btitle']
        )
        try:
            param_list.append(param)
        except Exception, e:
            print insert_error % (brd['btitle'], post['pid']), e
            continue
    s.executemany(query, param_list)
    s.close()


def main():
    start_time = time.time()

    b_list = get_blist()
    for b in b_list:
        queue = Queue.Queue()
        bid = b['bid']
        total = b['total']
        distance = get_distance(bid, total)
        begin = distance[0]
        end = distance[1]
        steps = end

        for i in xrange(int(steps/20 + 1)):
            start = 1 + i*20
            queue.put(start)

        for i in xrange(10):
            t = PostThread(bid, queue)
            t.setDaemon(True)
            t.start()
        queue.join()
        print PostThread.post_list[::-1][0]
        save_posts(b, PostThread.post_list)
        print b['bname'], "------> finished!"

    print 'This script cost', time.time() - start_time, 's'

if __name__ == '__main__':
    main()
