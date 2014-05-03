#!/usr/bin/env python
# -*- coding=utf-8 -*-

import MySQLdb
import config
import json
import random

class Sql():
    def __init__(self):
        self.conn = MySQLdb.connect(host=config.host, user=config.user,
                                    passwd=config.passwd, db=config.db_name,
                                    port=config.port, charset=config.charset)
        self.cursor = self.conn.cursor()

    def execute(self, query, param):
        self.cursor.execute(query, param)
        self.conn.commit()

    def executemany(self, query, param_list):
        self.cursor.executemany(query, param_list)
        self.conn.commit()

    def searchall(self, query):
        self.cursor.execute(query)
        fetch = self.cursor.fetchall()
        return fetch

    def close(self):
        self.cursor.close()
        self.conn.close()


class Proxy:
    def __init__(self):
        with open('proxy_list.json', 'r') as f:
            self.proxy_list = json.loads(f.read())

    def get_proxy(self):
        proxy = {'http': 'http://' + random.choice(self.proxy_list)}
        return proxy
