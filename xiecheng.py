#coding: utf8
import os
import sys
import re
import urllib
import logging
import pdb
import subprocess

import autopath
from db_config import *

from utils.common_handler import CommonHandler
from utils.http_client import HttpClient
from utils.btlog import btlog_init
from db.mysqlv6 import MySQLOperator

'''
drop table if exists test.xiecheng_landmark;
create table test.xiecheng_landmark (
id int auto_increment,
name varchar(64) default '',
format_name varchar(64) default '',
city varchar(64) default '',
similar_landmark varchar(64) default '',
similar_landmarkid int default 0,
primary key(id)
) engine=InnoDB, charset='utf8';
'''

class XiechengProcessor(CommonHandler, HttpClient):
    FULL_MATCH = ('火车站', '汽车站')
    RTRIM = ('周围', '周边', '附近', '旁边')
    REPLACE_WORD = ('旅馆', '宾馆', '酒店', '客栈', '预订网',
                     '查询', '预订', '预定', '便宜', '订房', '定房', '住宿',
                     '团购', '电话', '房价', '公寓', '攻略', '网上', '推荐',)
    def __init__(self):
        self.db_conn = MySQLOperator()
        if not self.db_conn.Connect(**PRODUCT_DATABASE):
            print 'db error'
            sys.exit()

        self.sem_conn = MySQLOperator()
        if not self.sem_conn.Connect(**SEM_DATABASE):
            print 'db error 2'
            sys.exit()

        self.city_dict = {}
        sql = "select * from hotel_city_basic where type_level not in ('区', '省');"
        result_set = self.sem_conn.QueryDict(sql)
        for row in result_set:
            self.city_dict[self.ToString(row['city_name'])] = row

        self.city_list = self.city_dict.keys()

        sql = "select id,name from biz_landmark"
        result_set = self.sem_conn.QueryDict(sql)
        self.landmark_dict = {}
        for row in result_set:
            self.landmark_dict[self.ToString(row['name'])] = row['id']
        self.landmark_list = self.landmark_dict.keys()

    def do_file(self, file_name):
        line_list = self.LoadList(file_name, "\r")
        logging.info("file_name: %s, line length: %d" % (file_name, len(line_list)))

        for line in line_list:
            line = line.strip()
            if len(line) == 0:
                continue
            db_data = {}
            db_data['name'] = line
            ret = self.db_conn.ExecuteInsertDict('test.xiecheng_landmark', db_data)
            if ret <= 0:
                logging.warn("invalid for line: [%s]" % line)

    def run(self):
        for i in (1,2,3):
            file_name = "xiecheng/%d.csv.utf8" % i
            self.do_file(file_name)

    def _rtrim(self, keyword):
        for word in self.RTRIM:
            pos = keyword.find(word)
            if pos > 0:
                keyword = keyword[:pos]
        return keyword

    def _replace(self, keyword):
        for word in self.REPLACE_WORD:
            keyword = keyword.replace(word, '')
        return keyword

    def _full_match(self, keyword):
        if keyword in self.FULL_MATCH:
            return ""
        return keyword

    def _ltrimcity(self, keyword):
        for city in self.city_list:
            if keyword.startswith(city):
                keyword = keyword[len(city):]
                return (city, keyword)
        return (None, keyword.strip())

    def _search_landmark(self, keyword_dict):
        for landmark in self.landmark_list:
            pos = landmark.find(keyword_dict['format_name'])
            if pos >= 0:
                keyword_dict['similar_landmarkid'] = self.landmark_dict[landmark]
                keyword_dict['similar_landmark'] = landmark

    def process_dup(self):
        sql = "select id,concat(city,format_name) format_name from test.xiecheng_landmark "
        result_set = self.db_conn.QueryDict(sql)

        name_list = []
        for row in result_set:
            if row['format_name'] in name_list:
                logging.info("delete id: %d, format_name: %s" % (row['id'],
                                                                 self.ToString(row['format_name'])))
                sql = "delete from test.xiecheng_landmark where id=%d" % row['id']
                self.db_conn.Execute(sql)
            else:
                name_list.append(row['format_name'])

    def parse(self):
        sql = "select id,name from test.xiecheng_landmark "
        result_set = self.db_conn.QueryDict(sql)
        logging.info(sql)
        logging.info("result_set len: %d" % len(result_set))
        for row in result_set:
            origin_keyword = self.ToString(row['name'])
            logging.info("process id: %d, name: %s" % (row['id'], row['name']))
            format_keyword = self._rtrim(origin_keyword)
            format_keyword = self._replace(format_keyword)
#            format_keyword = self._full_match(format_keyword)

            (city, keyword) = self._ltrimcity(format_keyword)
            db_dict = {}
            if city:
                db_dict['city'] = city
            if len(keyword) < 5:
                db_dict['kxflag'] = 'invalid'
                db_dict['reason'] = 'too short'
            db_dict['format_name'] = keyword
            self._search_landmark(db_dict)

            self.db_conn.ExecuteUpdateDict('test.xiecheng_landmark', db_dict, {'id': row['id']})


if __name__ == '__main__':
    btlog_init('encode.log', console=True, logfile=True)
    e = XiechengProcessor()
    e.parse()
