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
from db_config import *

from db.mysqlv6 import MySQLOperator
from utils.http_client import HttpClient
from utils.common_handler import CommonHandler
from utils.btlog import btlog_init


class KeywordHotelRelation(CommonHandler):
    RE = re.compile('_(\d{8})')
    KEYWORD_SERVLET = 'sem_keyword_servlet'

    def __init__(self):
        self.product_conn           = MySQLOperator()
        if not self.product_conn.Connect(**PRODUCT_DATABASE):
            logging.error("can not connect [%s]" % str(PRODUCT_DATABASE))
            sys.exit()

        self.sem_conn = MySQLOperator()
        if not self.sem_conn.Connect(**SEM_DATABASE):
            logging.error("can not connect [%s]" % str(SEM_DATABASE))
            sys.exit()

        parser = OptionParser()
        parser.add_option('--account', action='store', help="baidu-$$2732150-7:1, baidu-$$2116043-31:30, baidu-$$2116043-29:32, baidu-$$2732150-10:9",
                         choices=["1", "30", "32", "9"])
        parser.add_option('--full', action='store_true')
        parser.add_option('--commit', action='store_true')
        (self.opt, others) = parser.parse_args()


    def DoAccount(self, accountid):
        sql = "select kwid from %s where accountid=%s order by kwid asc" % (self.KEYWORD_SERVLET, accountid)
        mapping_result_set = self.sem_conn.Query(sql)
        logging.info("mapping keywordid result_set len: %d" % len(mapping_result_set))

        sql = ("select k.id,k.keywordid,k.accountid,g.adgroupname from sem_keyword k, sem_adgroup g "
         " where k.adgroupid=g.id and k.accountid=%s order by k.keywordid asc") % accountid
        logging.info("sql: %s" % sql)

        sem_keywordid_list = []
        sem_keywordid_dict = {}
        result_set = self.sem_conn.QueryDict(sql)
        logging.info("sem system: keyword count: %d for account: %s" % (len(result_set), accountid))
        for row in result_set:
            sem_keywordid_list.append([row['keywordid'],])
            sem_keywordid_dict[row['keywordid']] = row

        (only_exists_in_mapping, both_exists, only_exists_in_sem) = self.DiffList(mapping_result_set, sem_keywordid_list)
        logging.info("only_exists_in_maping len: %d" % len(only_exists_in_mapping))
        logging.info("both_exists len: %d" % len(both_exists))
        logging.info("only_exists_in_sem len: %d" % len(only_exists_in_sem))

        for keywordid_list in only_exists_in_sem:
            keywordid = keywordid_list[0]
            info_dict = sem_keywordid_dict[keywordid]
            r = self.RE.findall(info_dict['adgroupname'])
            if r and len(r) > 0:
                logging.warn("MATCH keywordid: %d, adgroupname: %s" % (info_dict['keywordid'], info_dict['adgroupname']))
                sql = ("insert into %s(kwid,accountid,hotelid) "
                        " values (%d,%s,%s);") % (self.KEYWORD_SERVLET, keywordid, accountid, r[0])
                logging.info("AAAA: %s" % sql)
                if self.opt.commit:
                    self.sem_conn.Execute(sql)
            else:
                logging.warn("SKIP keywordid: %d, adgroupname: %s" % (info_dict['keywordid'], info_dict['adgroupname']))

        '''
        if self.opt.commit:
            for keywordid in only_exists_in_mapping:
                sql = "delete from %s where kwid = %d" % (self.KEYWORD_SERVLET, keywordid)
                self.sem_conn.Execute(sql)
        '''

    def Run(self):
        if self.opt.account:
            self.DoAccount(self.opt.account)
        if self.opt.full:
            for account in ("1", "30", "32"):
                self.DoAccount(account)


    def test(self):
        l1 = [1,2,3,4,5,6,7,8,9]
        l2 = [4,6,8,9, 200, 201]
        print self._Diff(l1, l2)

if __name__ == '__main__':
    btlog_init('log/log_keyword_hotel_relation.log', logfile=True, console=True)
    k = KeywordHotelRelation()
    k.Run()
