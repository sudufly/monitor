#!/usr/bin/python
# coding:utf-8
import os
import sys
from HTMLParser import HTMLParser
from datetime import datetime

import requests


class MyHTMLParser(HTMLParser):
    def __init__(self, save, path):
        HTMLParser.__init__(self)
        self.interested_tags = {'pre', 'a'}  # 定义感兴趣标签的集合
        self.current_tag = None

    def handle_starttag(self, tag, attrs):
        if tag in self.interested_tags:
            # print("开始标签: <{}>".format(tag))
            if attrs:
                a = 1
                # print("属性: {}".format(dict(attrs)))
            self.current_tag = tag

    def handle_endtag(self, tag):
        if tag in self.interested_tags:
            # print("结束标签: </{}>".format(tag))
            self.current_tag = None

    def handle_data(self, data):
        if self.current_tag:
            stripped_data = data.strip().encode("utf-8")
            if stripped_data:
                if save == 0:
                    print(stripped_data)
                else:
                    with open(path, 'a') as file:
                        file.writelines(stripped_data)


b_url = sys.argv[1]
offset = -24096
save = 0
if len(sys.argv) == 3:
    offset = sys.argv[2]
if len(sys.argv) == 4:
    offset = sys.argv[2]
    save = int(sys.argv[3])

dir = "./logs"
if not os.path.exists(dir):
    os.makedirs(dir)
url = "{}?start={}".format(b_url, offset)

path = "logs/{}.log".format(datetime.now().strftime('%Y%m%d%H%M%S'))
parser = MyHTMLParser(save, path)
# print url
response = requests.get(url)
response.encoding = 'utf-8'
if response.status_code == 200:
    parser.feed(parser.unescape(response.text))
    if save != 0:
        print("日志保存完成,{}".format(path))
else:
    print("查询集群信息失败")
