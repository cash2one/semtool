import os
import sys
import re
import urllib
import logging

import autopath

from utils.common_handler import CommonHandler

class EncodeChinese(CommonHandler):

    def run(self, source_file):
        line_list = self.LoadList(source_file)
        logging.info("source len: %d" % len(line_list))

        dest_list = []
        for line in line_list:
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

        print items[1]
        chinese_str = re.match(r'.*-(.*)-jiudian', items[1]).group(1)
        encoded_str = urllib.quote(chinese_str)
        new_url = items[1].replace(chinese_str, encoded_str)
        s = "%s\t%s" % (items[0], new_url)
        return s

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'useage: %s filename' % sys.argv[0]
        sys.exit()

    e = EncodeChinese()
    e.run(sys.argv[1])
