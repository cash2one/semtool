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
import json
from datetime import datetime, timedelta
from optparse import OptionParser

import autopath

from utils.common_handler import CommonHandler
from utils.btlog import btlog_init
from db.mysqlv6 import MySQLOperator

PRODUCT_DATABASE = {
    "host"  : "192.168.0.57",
    "user"  : "product_w",
    "passwd": "kooxootest",
    "database"  : "product",
    "port"      : 3306,
    "charset"   : "utf8"
}

class MaterialGenerator(CommonHandler):
    RE = re.compile('_(\d{8})')

    TRIM_STR = (u'—', u'、', u'-', u'·', u'。', u'+', u'@', u'(', u')', u'（', u'）', u'（副楼）',
                u'（预付）', u'【', u'】', u'《', u'》', u'<', u'>' )

    REPLACE_DICT = {
        u'°' : u'度',
    }

    def __init__(self):
        self.product_conn           = MySQLOperator()
        if not self.product_conn.Connect(**PRODUCT_DATABASE):
            logging.error("can not connect [%s]" % str(PRODUCT_DATABASE))
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

    def GenerateMaterial(self, hotelid_list):
        result_hotel_list = []

        new_hotelid_list = [int(i) for i in hotelid_list]
        for hotelid in new_hotelid_list:
            sql = ("select hotelid,name,city,grade,comment_count,pinpai_name,ext_landingpage from tmp_hotel_info "
                    " where hotelid=%d and "
                    " length(city)>4 and length(ext_landingpage)>10" ) % hotelid
            result_set = self.product_conn.QueryDict(sql)
            if len(result_set) == 0:
                logging.warn("skip hotelid: %d" % hotelid)
                continue

            row = result_set[0]
            row['name'] = self._FormatHotelName(row['name'])
            tmp_str = "%(hotelid)d,%(name)s,%(city)s,%(grade)d,%(comment_count)d,%(pinpai_name)s,%(ext_landingpage)s" % row
            result_hotel_list.append(self.ToString(tmp_str))

        self.SaveList('new_%s' % sys.argv[1], result_hotel_list)

    def Run(self, filename):
        file_list = self.LoadList(filename)
        new_list = []
        for i in file_list:
            if len(i) > 0:
                new_list.append(i)

        self.GenerateMaterial(new_list)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print '%s filename' % sys.argv[0]
        sys.exit()

    btlog_init('log/material.log', logfile=False, console=True)
    k = MaterialGenerator()
    k.Run(sys.argv[1])
