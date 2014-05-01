#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import requests
import time
import json
import sql
import random
from bs4 import BeautifulSoup

reload(sys)
sys.setdefaultencoding('utf-8')

headers = {'User-Agent':
           'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.72 Safari/537.36'}
insert_error = '%s: %s fail to insert into mysql! -->'


def get_proxy():
    with open('proxy_list.json', 'r') as f:
        proxy_list = json.loads(f.read())
    proxy = {'http': 'http://' + random.choice(proxy_list)}
    return proxy


# 获取该board下所有帖子的标题及id
def get_plist(bid, plist_url):
    payload = {'bid': bid}
    while True:
        try:
            r = requests.get(plist_url, params=payload,
                             headers=headers, proxies=get_proxy())
        except Exception, e:
            print 'connection failure -->', e

        try:
            content = r.text
            soup = BeautifulSoup(content)
            brd = soup.find('brd')
            # 以主题为准的帖子总数
            total = brd['total']
            break
        except Exception, e:
            print 'get total failure! -->', e
            continue

    posts_list = []
    for i in xrange(int(total) / 20):
        payload = {'bid': bid, 'start': str(i * 20)}
        while True:
            try:
                r = requests.get(plist_url, params=payload,
                                 headers=headers)
                content = r.text
                soup = BeautifulSoup(content)
                po_list = soup.find_all('po')
                for po in po_list:
                    post_dict = {}
                    # 获取帖子的id
                    post_dict['pid'] = po['id']
                    # 获取贴子的标题
                    post_dict['title'] = po.string
                    # 将帖子的信息装入posts列表
                    posts_list.append(post_dict)
                    print post_dict['title']
                time.sleep(0.5)
                break
            except Exception, e:
                print 'get post lists failure! -->', e
                continue

    return posts_list


def save_pinfo(binfo, posts_list):
    query = "REPLACE INTO bbs_info (bdesc, btitle, bid, ptitle, pid) VALUES (%s, %s, %s, %s, %s)"
    s = sql.Sql()
    for post in posts_list:
        sql_params = (
            binfo['desc'], binfo['btitle'], binfo['bid'],
            post['title'], post['pid']
        )
        try:
            s.insert(query, sql_params)
        except Exception, e:
            print insert_error % (binfo['btitle'], post['pid']), e
            continue
    s.close()


def main():
    # 遍历获取帖子标题url
    plist_url = 'http://bbs.fudan.edu.cn/bbs/tdoc'
    with open('board_info.json', 'r') as f:
        binfo_list = json.loads(f.read())
    start = time.time()
    for binfo in binfo_list:
        if int(binfo['bid']) > 72:
            posts_list = get_plist(binfo['bid'], plist_url)
            save_pinfo(binfo, posts_list)
    print 'This script cost', time.time() - start, 's'

if __name__ == '__main__':
    main()
