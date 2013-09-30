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

PRODUCT_DATABASE = {
    "host"  : "192.168.0.57",
    "user"  : "product_w",
    "passwd": "kooxootest",
    "database"  : "product",
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
    KEYWORD_SERVLET = 'sem_keyword_servlet'

    def __init__(self):
        self.mapping_keywordid_list = []
        self.product_conn           = MySQLOperator()
        if not self.product_conn.Connect(**PRODUCT_DATABASE):
            logging.error("can not connect [%s]" % str(PRODUCT_DATABASE))
            sys.exit()

        self.sem_conn = MySQLOperator()
        if not self.sem_conn.Connect(**SEM_DATABASE):
            logging.error("can not connect [%s]" % str(SEM_DATABASE))
            sys.exit()

        parser = OptionParser()
        parser.add_option('--account', action='store', help="baidu-$$2732150-7:1, baidu-$$2116043-31:30, baidu-$$2116043-29:32",
                         choices=["1", "30", "32"])
        parser.add_option('--full', action='store_true')
        parser.add_option('--add_word', action='store_true')
        parser.add_option('--commit', action='store_true')
        (self.opt, others) = parser.parse_args()

    def _Diff(self, queue_list1, queue_list2):
        queue_idx1 = 0
        queue_idx2 = 0
        queue_length1 = len(queue_list1)
        queue_length2 = len(queue_list2)

        only_exists_in_list1 = []
        only_exists_in_list2 = []
        both_exists = []
        while queue_idx1 < queue_length1 and queue_idx2 < queue_length2:
            if queue_list1[queue_idx1] < queue_list2[queue_idx2]:
                only_exists_in_list1.append(queue_list1[queue_idx1])
                queue_idx1 += 1
            elif queue_list1[queue_idx1] > queue_list2[queue_idx2]:
                only_exists_in_list2.append(queue_list2[queue_idx2])
                queue_idx2 += 1
            else:
                both_exists.append(queue_list1[queue_idx1])
                queue_idx1 += 1
                queue_idx2 += 1

        while queue_idx1 < queue_length1:
            only_exists_in_list1.append(queue_list1[queue_idx1])
            queue_idx1 += 1

        while queue_idx2 < queue_length2:
            only_exists_in_list2.append(queue_list2[queue_idx2])
            queue_idx2 += 1

        return (only_exists_in_list1, both_exists, only_exists_in_list2)

    def DoAccount(self, accountid):
        self.mapping_keywordid_list   = []
        sql = "select kwid from %s where accountid=%s order by kwid asc" % (self.KEYWORD_SERVLET, accountid)
        tmp_result_set = self.sem_conn.Query(sql)
        for row in tmp_result_set:
            self.mapping_keywordid_list.append(row[0])
        logging.info("mapping keywordid len: %d" % len(self.mapping_keywordid_list))

        sql = ("select k.id,k.keywordid,k.accountid,g.adgroupname from sem_keyword k, sem_adgroup g "
         " where k.adgroupid=g.id and k.accountid=%s order by k.keywordid asc") % accountid
        logging.info("sql: %s" % sql)

        sem_keywordid_list = []
        sem_keywordid_dict = {}
        result_set = self.sem_conn.QueryDict(sql)
        logging.info("sem system: keyword count: %d for account: %s" % (len(result_set), accountid))
        for row in result_set:
            sem_keywordid_list.append(row['keywordid'])
            sem_keywordid_dict[row['keywordid']] = row

        (only_exists_in_mapping, both_exists, only_exists_in_sem) = self._Diff(self.mapping_keywordid_list, sem_keywordid_list)
        logging.info("only_exists_in_maping len: %d" % len(only_exists_in_mapping))
        logging.info("both_exists len: %d" % len(both_exists))
        logging.info("only_exists_in_sem len: %d" % len(only_exists_in_sem))

        if not self.opt.commit:
            return

        for keywordid in only_exists_in_sem:
            info_dict = sem_keywordid_dict[keywordid]
            r = self.RE.findall(info_dict['adgroupname'])
            if r and len(r) > 0:
                logging.warn("MATCH keywordid: %d, adgroupname: %s" % (info_dict['keywordid'], info_dict['adgroupname']))
                sql = ("insert into %s(kwid,accountid,hotelid) "
                        " values (%d,%s,%s)") % (self.KEYWORD_SERVLET, keywordid, accountid, r[0])
                logging.info("AAAA: %s" % sql)
                self.sem_conn.Execute(sql)
            else:
                logging.warn("SKIP keywordid: %d, adgroupname: %s" % (info_dict['keywordid'], info_dict['adgroupname']))

        for keywordid in only_exists_in_mapping:
            sql = "delete from %s where kwid = %d" % (self.KEYWORD_SERVLET, keywordid)
            self.sem_conn.Execute(sql)

    def HotelNotInKeyword(self):
        mapping_hotelid_list    = []
        sql = "select distinct hotelid from %s order by hotelid asc" % self.KEYWORD_SERVLET
        result_set = self.sem_conn.Query(sql)
        for row in result_set:
            mapping_hotelid_list.append(row[0])
        logging.info("mapping hotelid count: %d" % len(mapping_hotelid_list))

        product_hotelid_list    = []
        sql = ("select distinct i.hotelid from tmp_hotel_info i, tmp_hotel_price p "
                " where p.hotelid = i.hotelid order by hotelid asc")
        result_set = self.product_conn.Query(sql)
        for row in result_set:
            product_hotelid_list.append(row[0])
        logging.info("there are total hotel in product db: %d" % len(product_hotelid_list))

        (only_exists_in_mapping, both_exists, only_exists_in_product) = self._Diff(mapping_hotelid_list, product_hotelid_list)
        logging.info("only exists in mapping: %d" % len(only_exists_in_mapping))
        logging.info("both exists: %d" % len(both_exists))
        logging.info("only exists in product: %d" % len(only_exists_in_product))

        tmp_list = [str(i) for i in only_exists_in_product]
        self.SaveList('product_hoteid.txt', tmp_list)

    def Run(self):
        if self.opt.account:
            self.DoAccount(self.opt.account)
        if self.opt.full:
            for account in ("1", "30", "32"):
                self.DoAccount(account)

        if self.opt.add_word:
            self.HotelNotInKeyword()

    def test(self):
        l1 = [1,2,3,4,5,6,7,8,9]
        l2 = [4,6,8,9, 200, 201]
        print self._Diff(l1, l2)

if __name__ == '__main__':
    btlog_init('log/sem_keyword_servlet.log', logfile=False, console=True)
    k = KeywordHotelidRelationship()
    k.Run()
