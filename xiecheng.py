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
city varchar(64) default '',
similar_landmark varchar(64) default '',
similar_landmarkid int default 0,
primary key(id)
) engine=InnoDB, charset='utf8';
'''

class XiechengProcessor(CommonHandler, HttpClient):
    def __init__(self):
        self.db_conn = MySQLOperator()
        if not self.db_conn.Connect(**PRODUCT_DATABASE):
            print 'db error'
            sys.exit()

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

if __name__ == '__main__':
    btlog_init('encode.log', console=True, logfile=False)
    e = XiechengProcessor()
    e.run()
