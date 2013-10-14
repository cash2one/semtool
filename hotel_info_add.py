#!/usr/bin/python
# coding: utf-8

'''
drop table if exists tmp_hotel_info_add;
CREATE TABLE `tmp_hotel_info_add` (
    `hotelid` int(11) NOT NULL default '0',
    `name` varchar(256) default NULL,
    `formated_name` varchar(256) default NULL,
    `city_name` varchar(256) default NULL,
    `city` varchar(64) default NULL,
    `grade` int(11) default NULL,
    `comment_count` int(11) default NULL,
    `pinpai_name` varchar(64) default NULL,
    `ext_landingpage` varchar(256) default NULL,
    PRIMARY KEY  (`hotelid`)
    ) ENGINE=MyISAM DEFAULT CHARSET=utf8
'''

import os
import sys
import time
import urllib
import logging
import pdb
import traceback
import re
import json
from datetime import datetime, timedelta
from optparse import OptionParser

import autopath
from db_config import *

from utils.common_handler import CommonHandler
from utils.btlog import btlog_init
from db.mysqlv6 import MySQLOperator

class HotelInfoAdd(CommonHandler):
    RE = re.compile('_(\d{8})')

    TRIM_STR = (u'—', u'、', u'-', u'·', u'。', u'+', u'@', u'(', u')', u'（', u'）', u'（副楼）',
                u'（预付）', u'【', u'】', u'《', u'》', u'<', u'>', u'★' )

    REPLACE_DICT = {
        u'°' : u'度',
    }

    KEYWORD_SERVLET = 'sem_keyword_servlet'

    def __init__(self):
        self.product_conn           = MySQLOperator()
        if not self.product_conn.Connect(**PRODUCT_DATABASE):
            logging.error("can not connect [%s]" % str(PRODUCT_DATABASE))
            sys.exit()

        self.sem_conn           = MySQLOperator()
        if not self.sem_conn.Connect(**SEM_DATABASE):
            logging.error("can not connect [%s]" % str(SEM_DATABASE))
            sys.exit()

        parser = OptionParser()
        parser.add_option('--test', action='store_true')
        (self.opt, others) = parser.parse_args()

    def _FormatHotelName(self, hotelname):
        if not isinstance(hotelname, unicode):
            hotelname = hotelname.decode('utf8', 'ignore')
        logging.info("hotelname: %s, %d" % (hotelname, len(hotelname)))

        # special 1
        pos = hotelname.find(u'（原')
        if pos >= 0:
            new_pos = hotelname.find(u'）', pos)
            if new_pos >= 0:
                hotelname = hotelname[:pos] + hotelname[new_pos:]
            else:
                hotelname = hotelname[:pos]

        for item in self.TRIM_STR:
            hotelname = hotelname.replace(item, '')

        for k,v in self.REPLACE_DICT.iteritems():
            hotelname = hotelname.replace(k,v)

        return hotelname

    def _CityName(self, row):
        if row['formated_name'].find(row['city']) >= 0:
            return ''
        else:
            return row['city'] + row['formated_name']

    def GenerateHotelInfoAdd(self, hotelid_list):
        result_hotel_list = []

        sql = "delete from test.tmp_hotel_info_add"
        self.sem_conn.Execute(sql)

        new_hotelid_list = [int(i) for i in hotelid_list]
        for hotelid in new_hotelid_list:
            sql = ("select hotelid,name,city,grade,comment_count,pinpai_name,ext_landingpage from tmp_hotel_info "
                    " where hotelid=%d and "
                    " length(city)>4 and length(ext_landingpage)>10" ) % hotelid
            result_set = self.product_conn.QueryDict(sql)
            if len(result_set) == 0:
                logging.warn("skip hotelid: %d" % hotelid)
                continue

            row                     = result_set[0]
            row['formated_name']    = self._FormatHotelName(row['name'])
            row['city_name']        = self._CityName(row)
            self.sem_conn.ExecuteInsertDict('test.tmp_hotel_info_add', row)

    def NewHotel(self, filename):
        sql = "select distinct hotelid from %s order by hotelid asc" % self.KEYWORD_SERVLET
        mapping_result_set = self.sem_conn.Query(sql)
        logging.info("mapping hotelid count: %d" % len(mapping_result_set))

        sql = ("select distinct i.hotelid from tmp_hotel_info i, tmp_hotel_price p "
                " where p.hotelid = i.hotelid order by hotelid asc")
        hotel_result_set = self.product_conn.Query(sql)
        logging.info("there are total hotel in product db: %d" % len(hotel_result_set))

        (only_exists_in_mapping, both_exists, only_exists_in_product) = self.DiffList(mapping_result_set, hotel_result_set)
        logging.info("only exists in mapping: %d" % len(only_exists_in_mapping))
        logging.info("both exists: %d" % len(both_exists))
        logging.info("only exists in product: %d" % len(only_exists_in_product))

        tmp_list = [str(i[0]) for i in only_exists_in_product]
        self.SaveList(filename, tmp_list)


    def Run(self, filename="new_product_hotelid.txt"):
        self.NewHotel(filename)

        file_list = self.LoadList(filename)
        new_list = []
        for i in file_list:
            if len(i) > 0:
                new_list.append(i)

        self.GenerateHotelInfoAdd(new_list)


if __name__ == '__main__':
    btlog_init('log/log_hotel_info_add.log', logfile=False, console=True)
    k = HotelInfoAdd()
    k.Run()
