#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import requests
import random
import json
import time
from bs4 import BeautifulSoup

reload(sys)
sys.setdefaultencoding('utf-8')

headers = {'User-Agent':
           'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.72 Safari/537.36'}


def get_proxy():
    with open('proxy_list.json', 'r') as f:
        proxy_list = json.loads(f.read())
    proxy = {'http': 'http://' + random.choice(proxy_list)}
    return proxiesy


def get_btitle(blist_url):
    #login_data = {'id': 'zhangdongabb', 'pw': '929263'}
    try:
        #s = requests.Session()
        #s.post(login_url, data=login_data)
        r = requests.get(blist_url, headers=headers)
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
                'btitle': b['title'],
                'desc': b['desc'],
            })
    return btitle_list


# 获取每一个board的bid
def get_bid(btitle, bid_url):
    payload = {'board': btitle}
    proxy = get_proxy()
    while True:
        try:
            r = requests.get(bid_url, params=payload,
                             headers=headers, proxies=proxy, timeout=3)
            assert r.status_code == requests.codes.ok
            content = r.text
            soup = BeautifulSoup(content)
            binfo = soup.find('brd')
            assert binfo
            return binfo['bid']
        except Exception, e:
            print e
            proxy = get_proxy()
            continue
    time.sleep(0.5)


# 将获取的所有board的title，desc，bid信息保存
def save_binfo(binfo_list):
    with open('board_info.json', 'w+') as f:
        output = json.dumps(obj=binfo_list, ensure_ascii=False, indent=4)
        f.write(output)


def main():
    # 登陆获取cookies
    # login_url = 'http://bbs.fudan.edu.cn/bbs/login'
    # 获取版面列表url
    blist_url = 'http://bbs.fudan.edu.cn/bbs/all'
    # 版面为board目录的url
    dir_url = 'http://bbs.fudan.edu.cn/bbs/boa'
    # 获取版面bid url
    bid_url = 'http://bbs.fudan.edu.cn/bbs/doc'
    # 每个元素为包含了board的title, desc的字典
    btitle_list = get_btitle(blist_url)
    for i in range(len(btitle_list)):
        bid = get_bid(btitle_list[i]['btitle'], bid_url)
        btitle_list[i]['bid'] = bid
        print i + 1, btitle_list[i]
    save_binfo(btitle_list)


if __name__ == '__main__':
    main()
