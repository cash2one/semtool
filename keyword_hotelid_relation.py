#!/usr/bin/python
# coding: utf-8

import os
import sys
import time
import urllib
import logging
import pdb
import traceback
import re
import threading
from datetime import datetime, timedelta
from optparse import OptionParser

try:
    import simplejson as json
except:
    try:
        import json
    except:
        print 'no json'
        sys.exit()

FILE_PATH = os.path.realpath(os.path.dirname(__file__))

import autopath

from db.mysqlv6 import MySQLOperator
from utils.http_client import HttpClient
from utils.common_handler import CommonHandler
from utils.btlog import btlog_init

TEST_DATABASE = {
    "host"  : "192.168.0.57",
    "user"  : "product_w",
    "passwd": "kooxootest",
    "database"  : "test",
    "port"      : 3306,
    "charset"   : "utf8"
}

SEM_DATABASE = {
    "host"  : "192.168.0.233",
    "user"  : "sem",
    "passwd": "sem@Kooxoo1126",
    "database"  : "new_sem_db",
    "port"      : 3306,
    "charset"   : "utf8"
}


class KeywordHotelidRelationship(CommonHandler):
    RE = re.compile('_(\d{8})')

    def __init__(self):
        self.dest_db_conf = TEST_DATABASE
        self.dest_conn = MySQLOperator()
        if not self.dest_conn.Connect(**self.dest_db_conf):
            logging.error("can not connect [%s]" % str(self.dest_db_conf))
            sys.exit()

        self.sem_conn = MySQLOperator()
        if not self.sem_conn.Connect(**SEM_DATABASE):
            logging.error("can not connect [%s]" % str(SEM_DATABASE))
            sys.exit()


        parser = OptionParser()
        parser.add_option('--account', action='store', help="baidu-$$2732150-7:1, baidu-$$2116043-31:30, baidu-$$2116043-29:32",
                         choices=["1", "30", "32"])
        parser.add_option('--full', action='store_true')
        (self.opt, others) = parser.parse_args()

    def DoUpdate(self, row, hotelid):
        sql = "select count(*) cnt from sem_keyword_servlet where kwid=%d" % row['keywordid']
        count = self.sem_conn.QueryDict(sql)[0]
        if count > 0:
            logging.info("already exists for keywordid: %d" % row['keywordid'])
            return

        sql = ("insert into sem_keyword_servlet(kwid,accountid,hotelid) "
                " values (%d,%d,%s)") % (row['keywordid'], row['accountid'], hotelid)
        logging.info("AAAA: %s" % sql)
#        self.sem_conn.Execute(sql)

    def DoAccount(self, accountid):
        sql = ("select k.id,k.keywordid,k.accountid,g.adgroupname from sem_keyword k, sem_adgroup g "
         " where k.adgroupid=g.id and k.accountid=%s") % accountid
        logging.info("sql: %s" % sql)

        result_set = self.sem_conn.QueryDict(sql)
        logging.info("keyword count: %d for account: %s" % (len(result_set), account))
        for row in result_set:
            r = self.RE.findall(row['adgroupname'])
            if r and len(r) > 0:
                print type(r[0]), r[0]
                logging.info("adgroup id: %s, adgroupname: %s, hotelid: %s" % (
                        self.ToString(row['id']), self.ToString(row['adgroupname']), self.ToString(r[0])))
                self.DoUpdate(row, r[0])
            else:
                logging.warn("skip adgroup id: %d, adgroupname: %s" % (row['id'], self.ToString(row['adgroupname'])))

    def Run(self):
        if self.opt.account:
            self.DoAccount(self.opt.account)
        if self.opt.full:
            for account in ("1", "30", "32"):
                self.DoAccount(account)

if __name__ == '__main__':
    btlog_init('log/tool.log', logfile=True, console=True)
    k = KeywordHotelidRelationship()
    k.Run()
