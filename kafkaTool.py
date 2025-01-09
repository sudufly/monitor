#!/usr/bin/python
# coding:utf-8
from collections import OrderedDict

from kafka import KafkaAdminClient

from common import common as cm
from component.kafka_tools import KafkaUtil
from config.config import Config


class KafkaTool(object):
    config = Config()
    kafka_admin_client = None
    kafka_util = KafkaUtil()
    context = ""
    level = 0

    preCmd = [0]
    fun = []

    def __init__(self):
        bootstrapServers = self.config.get_bootstrap_servers()
        self.kafka_admin_client = KafkaAdminClient(bootstrap_servers=bootstrapServers)

    def group_progress(self, cmd):
        # print("cmd:{}".format(cmd))
        idx = -1
        split = cmd.split(" ")
        cmd = split[0]
        tpoic = ''
        if len(split) == 2:
            topic = split[1]

        if cmd.isdigit():
            idx = int(cmd) - 1

        pre = self.preCmd[self.level]
        if idx >=0 and idx < len(pre):
            info = pre[idx]

            gid = info['groupId']
            kafka_set = set()
            print 'gid:{}'.format(gid)

            consumer_offset_map = self.kafka_util.list_consumer_group_offsets(gid)
            [kafka_set.add(v.topic)
             for v in consumer_offset_map.keys()
             ]
            kafka_offset_map = {}
            for topic in kafka_set:
                r = self.kafka_util.get_topic_offsets(self.kafka_util.client, topic)
                kafka_offset_map.update(r[0])
            group_info = self.kafka_util.get_consumer_group_clients(gid)
            # print group_info
            arr = []
            idx = 0
            l = set()
            l.update(group_info.keys())
            l.update(consumer_offset_map.keys())
            list = sorted(l, key=lambda x: (x.topic, x.partition))  # 按名称升序，数量降序（负数）

            for k in list:
                v = consumer_offset_map.get(k, None)
                if len(topic) > 0 and k.topic != topic:
                    continue
                idx += 1
                range = kafka_offset_map.get(k, 0)
                cur_offset = v.offset if v is not None else '-'
                lag = int(range) - int(cur_offset) if v is not None else '-'
                cli = group_info.get(k, ('-', '-', '-'))
                cur_offset = v.offset if v is not None else '-'
                arr.append(OrderedDict([
                    ("id", idx),
                    ("groupId", gid),
                    ('topic', k.topic),
                    ('partition', k.partition),
                    ('current offset', cur_offset),
                    ('max offset', range),
                    ('lag', lag),
                    ('clientId', cli[0]),
                    ('memberId', cli[1]),
                    ('host', cli[2]),
                ]))

            cm.print_dataset('Partition Offset List', arr)
            print
        else:
            print (">> 输入错误,重新输入")
        cm.print_dataset("GroupId List", pre)
        print(">> 按[B]返回上一级")

    def group(self):
        groups = []
        ids = self.kafka_admin_client.list_consumer_groups()
        idx = 0

        for id in ids:
            idx += 1
            groups.append({'id': idx, 'groupId': id[0]})
        cm.print_dataset('GroupId List', groups)
        pre_context = self.context
        self.context = (">> 请选择消费组id: ")
        self.level = 1
        self.preCmd.insert(self.level, groups)
        # self.preCmd.insert(self.level, {'cmd': self.user_input, 'ret': groups})

        while True:
            if self.level == 0:
                break
            user_input = raw_input(self.context)
            if len(user_input) == 0:
                continue
                # 检查是否需要退出循环
            if user_input == 'exit':
                print("退出程序...")
                break
            back = False
            if user_input == 'B' or user_input == 'b':
                self.level -= 1
                break

            # 处理用户输入
            # if level == 0 or is_number(user_input):
            self.group_progress(user_input)
        self.context = pre_context

    def topic(self):
        print"topic"

    def list(self):
        l = []

        l.append(('group ', self.group))

        l.append(('topic ', self.topic))

        return l

    def loop(self):

        total_length = 90

        print("*" * total_length)
        print('' + "kafkaTool".center(total_length - 3) + '')
        print("*" * total_length)
        print('' + "输入 'exit' 退出程序")
        print('' + "按[B]返回上一级")
        print('' + "1. group: 查看消费组消费表")
        print("*" * total_length)
        print("*" * total_length)
        print("")
        self.fun = self.list()
        arr = []
        idx = 0
        for i in self.fun:
            idx += 1
            arr.append(OrderedDict([
                ("id", idx),
                ('function', i[0])
            ]))
        # print funx
        cm.print_dataset('Function List', arr)

        level = 0
        user_input = ''
        self.context = '请选择功能id: '
        self.preCmd.insert(self.level, arr)
        while True:
            try:
                user_input = raw_input(self.context)
                if len(user_input) == 0:
                    continue

                # 检查是否需要退出循环

                if user_input == 'exit':
                    print("退出程序...")
                    break
                back = False
                if user_input == 'B' or user_input == 'b':
                    level -= 2
                    back = True
                # 处理用户输入
                # if level == 0 or is_number(user_input):
                self.process_command(user_input, back)


            except KeyboardInterrupt:
                print("\n检测到 Ctrl+C，正在退出程序...")
                break

    def process_command(self, cmd, back):
        print('>> 当前输入:{}'.format(cmd))
        if self.level == 0:
            idx = int(cmd) - 1
            self.fun[idx][1]()
            cm.print_dataset('Function List', self.preCmd[0])
            self.level = 0


if __name__ == "__main__":
    kafkaTool = KafkaTool()
    kafkaTool.loop()
