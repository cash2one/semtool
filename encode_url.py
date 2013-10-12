#coding: utf8
import os
import sys
import re
import urllib
import logging
import pdb
import subprocess

import autopath

from utils.common_handler import CommonHandler
from utils.http_client import HttpClient
from utils.btlog import btlog_init

'''
file format:
    $heaer (one line)
    $content (multi line)
suggest file encoding: gbk
'''

class EncodeChinese(CommonHandler, HttpClient):

    def is_mutichaset(self, s):
        if isinstance(s, unicode):
            raise Exception, 'param should is str'

        unicode_s = self.ToUnicode(s)
        if len(unicode_s) == len(s):
            return False
        return True

    def run(self, source_file):
        if sys.argv[2] == 'gbk':
            dest_file = source_file + ".utf8"
            subprocess.Popen("iconv -c -f gbk -t utf8 %s > %s" % (source_file, dest_file),
                             shell=True, stdout=subprocess.PIPE).communicate()
            self.do_file(source_file)
            self.do_file(dest_file)
        elif sys.argv[2] == 'utf8':
            dest_file = source_file + ".utf8"
            subprocess.Popen("iconv -c -f utf8 -t gbk %s > .tmp" % (source_file),
                             shell=True, stdout=subprocess.PIPE).communicate()
            subprocess.Popen("iconv -c -f gbk -t utf8 .tmp > %s" % (dest_file),
                             shell=True, stdout=subprocess.PIPE).communicate()
            self.do_file(source_file)
            self.do_file(dest_file)


    def do_file(self, source_file):
        line_list = self.LoadList(source_file)
        logging.info("source len: %d" % len(line_list))

        detect_result = {}
        formated_list = []
        for line in line_list:
            line = line.strip("\n").strip("\r")
            if len(line) <= 1:
                continue
            formated_list.append(line)

        dest_list = []
        first_line_flag = True
        for line in formated_list:
            if first_line_flag: # skip header
                first_line_flag = False
                continue
            if len(line) <= 1:
                break # no empty line allowed
            new_line = self.do_line(line)
            dest_list.append(new_line)

        logging.info("dest_list len: %d" % len(dest_list))
        self.SaveList("%s.encode" % source_file, dest_list)

    def do_line(self, line):
        items = self.line_items(line)

        todo_str = re.match(r'.*-(.*)-jiudian', items[1]).group(1)
        if self.is_mutichaset(todo_str):
            uri = "/semtool/php/keyword_encoding.php?word=%s&flag=semtool" % todo_str
            encoded_str = self.DoGet('192.168.0.233', 80, uri)
            if len(encoded_str) < len(todo_str):
                raise Exception, 'encode api error'
            logging.info("E: %s => %s" % (todo_str, encoded_str))
            new_url = items[1].replace(todo_str, encoded_str)
            s = "%s\t%s" % (items[0], new_url)
        else:
            s = "%s\t%s" % (items[0], items[1])
        return s

    def line_items(self, line):
        items = line.split("\t")
        if len(items) != 2:
            print line, items
            raise Exception
        return items

class CsvProcessor(EncodeChinese):
    def line_items(self, line):
        items = line.split(",")
        if len(items) != 2:
            print line, items
            raise Exception
        return items

def test():
    c = CsvProcessor()
    for line in ('1,http://a.b.c/-北京-jiudian', '2,http://b.c.d/-a-jiudian', '3,http://a.b.c/-%E5%8C%97%E4%BA%AC-jiudian'):
        print c.do_line(line)
    
def usage():
    print 'useage: %s filename gbk|utf8' % sys.argv[0]
    sys.exit()

if __name__ == '__main__':
    if len(sys.argv) != 3 or sys.argv[2] not in ['gbk', 'utf8']:
        usage()

    btlog_init('log/encode.log')
    e = CsvProcessor()
    e.run(sys.argv[1])
