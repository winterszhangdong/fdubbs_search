#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import requests
import time
import utils
import Queue
import threading
from bs4 import BeautifulSoup

reload(sys)
sys.setdefaultencoding('utf-8')

headers = {'User-Agent':
           'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.72 Safari/537.36'}

# 登陆获取cookies
# login_url = 'http://bbs.fudan.edu.cn/bbs/login'
# 获取版面列表url
btitle_url = 'http://bbs.fudan.edu.cn/bbs/all'
# 获取版面bid url
bid_url = 'http://bbs.fudan.edu.cn/bbs/doc'
# 获取以主题为基准的版面帖子总数
total_url = 'http://bbs.fudan.edu.cn/bbs/tdoc'

queue = Queue.Queue()
b_list = []

def get_btitle():
    #login_data = {'id': 'zhangdongabb', 'pw': '929263'}
    try:
        #s = requests.Session()
        #s.post(login_url, data=login_data)
        r = requests.get(btitle_url, headers=headers)
    except Exception, e:
        print e
        exit(1)
    content = r.text
    soup = BeautifulSoup(content)
    brd_list = soup.find_all('brd')
    btitle_list = []
    for b in brd_list:
        if '1' == b['dir']:
            print "directory", b
            continue
        else:
            # desc为版面中文名, dir为1时说明该board为子board的目录
            btitle_list.append({
                'title': b['title'],
                'desc': b['desc'],
            })
    return btitle_list


# 获取每一个board的bid
def get_bid(btitle):
    payload = {'board': btitle}
    p = utils.Proxy()
    while True:
        try:
            r = requests.get(bid_url, params=payload,
                             headers=headers, proxies=p.get_proxy(), timeout=3)
            assert r.status_code == requests.codes.ok
            soup = BeautifulSoup(r.text)
            binfo = soup.find('brd')
            assert binfo
            return binfo['bid']
        except Exception, e:
            print e
            continue
    time.sleep(0.5)

def get_total(bid):
    payload = {'bid': bid}
    p = utils.Proxy()
    while True:
        try:
            r = requests.get(total_url, params=payload,
                             headers=headers, proxies=p.get_proxy(), timeout=3)
            assert r.status_code == requests.codes.ok
            soup = BeautifulSoup(r.text)
            brd = soup.find('brd')
            assert brd
            return brd['total']
        except Exception, e:
            print e
            continue
    time.sleep(0.5)


class BoardThread(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            board = self.queue.get()
            b_info = {}
            bid = get_bid(board['title'])
            total = get_total(bid)
            print board['desc'], "total: ", total
            b_info['bid'] = int(bid)
            b_info['title'] = board['title']
            b_info['desc'] = board['desc']
            b_info['total'] = int(total)
            b_list.append(b_info)
            self.queue.task_done()


# 将获取的所有board的title，desc，bid, total信息保存
def save_binfo(b_list):
    query = "REPLACE INTO board (bid, btitle, bname, total) VALUES (%s, %s, %s, %s)"
    s = utils.Sql()
    param_list = []
    for b in b_list:
        param = (b['bid'], b['title'], b['desc'], b['total'])
        param_list.append(param)
    s.executemany(query, param_list)
    s.close()


def main():
    start = time.time()
    btitle_list = get_btitle()
    for i in range(20):
        try:
            t = BoardThread(queue)
            t.setDaemon(True)
            t.start()
        except Exception, e:
            print e

    for b in btitle_list:
        queue.put(b, block=False)

    queue.join()
    save_binfo(b_list)
    print "Finished! Costs", time.time() - start, "s"

if __name__ == '__main__':
    main()
