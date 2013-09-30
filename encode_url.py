#coding: utf8
import os
import sys
import re
import urllib
import logging

import autopath

from utils.common_handler import CommonHandler

'''
file format:
    $heaer (one line)
    $content (multi line)
suggest file encoding: gbk
'''

class EncodeChinese(CommonHandler):

    def is_mutichaset(self, s):
        if isinstance(s, unicode):
            raise Exception, 'param should is str'

        unicode_s = self.ToUnicode(s)
        if len(unicode_s) == len(s):
            return False
        print 'matched ', s
        return True

    def run(self, source_file):
        line_list = self.LoadList(source_file)
        logging.info("source len: %d" % len(line_list))

        dest_list = []
        first_line_flag = True
        for line in line_list:
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
        items = line.split("\t")
        if len(items) != 2:
            print line
            raise Exception

        todo_str = re.match(r'.*-(.*)-jiudian', items[1]).group(1)
        if self.is_mutichaset(todo_str):
            encoded_str = urllib.quote(todo_str)
            new_url = items[1].replace(todo_str, encoded_str)
            s = "%s\t%s" % (items[0], new_url)
        else:
            s = "%s\t%s" % (items[0], items[1])
        return s

class CsvProcessor(EncodeChinese):
    def do_line(self, line):
        items = line.split(",")
        if len(items) != 2:
            print line
            print 'items len', len(items)
            print items
            raise Exception

        todo_str = re.match(r'.*-(.*)-jiudian', items[1]).group(1)
        if self.is_mutichaset(todo_str):
            encoded_str = urllib.quote(todo_str)
            new_url = items[1].replace(todo_str, encoded_str)
            s = "%s\t%s" % (items[0], new_url)
        else:
            s = "%s\t%s" % (items[0], items[1])
        return s

def test():
    c = CsvProcessor()
    for line in ('1,http://a.b.c/-北京-jiudian', '2,http://b.c.d/-a-jiudian', '3,http://a.b.c/-%E5%8C%97%E4%BA%AC-jiudian'):
        print c.do_line(line)
    

if __name__ == '__main__':
    test()
    sys.exit()

    if len(sys.argv) != 2:
        print 'useage: %s filename' % sys.argv[0]
        sys.exit()

    e = EncodeChinese()
    e.run(sys.argv[1])
