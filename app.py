#!/usr/bin/python
# coding:utf-8
import re
import subprocess
from collections import OrderedDict

import requests

from common import common as cm

# import pandas as pd


yarn_url = "http://172.16.0.12:8188"
# 0: 调用yarn container --list ${container}
# 1: 调用rest-api
version = 0
# yarn container --list 版本字段不同 v0 有http前缀
v = 0


def getConfig():
    global yarn_url
    global version
    global v
    config = cm.getConfig()
    ycfg = config['yarn']
    yarn_url = ycfg['rm_url']
    version = int(ycfg['version'])
    v = int(ycfg['v'])


def get_cluster_info():
    """获取YARN集群的资源信息"""
    url = "{}/ws/v1/cluster".format(yarn_url)
    response = requests.get(url)
    response.encoding = 'utf-8'
    if response.status_code == 200:
        data = response.json()
        # print(response.text)
        cluster_info = data['clusterInfo']
        print("集群名称:%s" % cluster_info['id'])
        # print("集群名称: {}".format(cluster_info['clusterName']))
        # print("总内存: {%s}" % cluster_info['totalMB'] "MB")
        # print("总VCores: {%s}" % cluster_info['totalVirtualCores'])
        print("集群状态:" + cluster_info['state'].encode("utf-8"))
    else:
        print("查询集群信息失败")


def get_app_list():
    """获取所有应用程序的详细信息"""
    # response = requests.get("{}/ws/v1/cluster/apps".format(yarn_url))
    url = "{}/ws/v1/cluster/apps?state=SUBMITTED, ACCEPTED, RUNNING".format(yarn_url)
    response = requests.get(url)
    dataset = []
    # print(url)
    if response.status_code == 200:
        apps_data = response.json()
        if apps_data['apps'] is None:
            return dataset
        applications = apps_data['apps']['app']
        # print("应用程序ID|}应用名|状态|type")
        fmt = "|%-5s|%-35s|%-60s|%-10s|%-20s|%-10s|%-10s|%-10s|"
        # print(fmt % ("id", "applicationId", "name", "status", "type", "allocMB", "VCores", "Containers"))
        idx = 0

        for app in applications:
            idx = idx + 1
            id = app['id']

            name = app['name'].encode('utf-8')
            progress = app['progress']
            utc_ms = app['startedTime']
            state = app['state'].encode('utf-8')
            type = app['applicationType'].encode('utf-8')
            allocatedMB = app['allocatedMB']
            vcores = app['allocatedVCores']
            containers = app['runningContainers']

            tmp = OrderedDict([
                ("id", idx),
                ("applicationId", id),
                ("name", name),
                ("status", state),
                ("type", type),
                ("startTime", cm.utc_ms_to_time(utc_ms)),
                ("progress", progress),
                ("allocMb", allocatedMB),
                ("VCores", vcores),
                ("Containers", containers),
            ])
            dataset.append(tmp)
        cm.print_dataset("Application List", dataset)

    else:
        print("查询应用列表失败")
    return dataset


def list_app(type, appid):
    url = "{}/ws/v1/cluster/apps/{}/appattempts".format(yarn_url, appid)
    response = requests.get(url)
    # map = {}
    dataset = []
    #    print url
    if response.status_code == 200:
        attempts_data = response.json()
        attempts = attempts_data['appAttempts']['appAttempt']
        # print("应用程序ID|}应用名|状态|type")
        fmt = "|%-5s|%-45s|%-30s|%-100s|"
        # print(fmt % ("id", "attemtpId", "host", "logs"))
        idx = 0

        for attempt in attempts:
            idx = idx + 1
            cid = attempt['containerId'].encode('utf-8')
            aid = attempt['id']
            utc_ms = attempt['startTime']

            split = cid.split("_")
            l = len(split)
            attemptId = "appattempt_%s_%s_%06d" % (split[l - 4], split[l - 3], aid)

            # map[idx] = attemptId
            host = attempt['nodeHttpAddress'].encode('utf-8')
            logs = attempt['logsLink'].encode('utf-8')
            tmp = OrderedDict([
                ("id", idx),
                ("attemptId", attemptId),
                ("host", host),
                ("startTime", cm.utc_ms_to_time(utc_ms)),
                ("logs", 'http:{}'.format(logs.replace('http:',''))),
            ])
            dataset.append(tmp)

            # print(fmt % (idx, attemptId, host, logs))
        cm.print_dataset("Attempts List", dataset)

    else:
        print("查询应用列表失败")
    return dataset


def parse_yarn_container_list(output):
    ret = []
    if version == 0 and v == 0:
        # 定义正则表达式模式，允许字段内包含空格
        pattern = re.compile(
            r'^(?P<containerId>\S+)\s+'  # 容器ID
            r'(?P<start_time>[^\t]+)\s+'  # 启动时间
            r'(?P<allocated_vcores>[^\t]+)\s+'  # 分配的VCores数量
            r'(?P<state>[^\t]+)\s+'  # 状态
            r'(?P<node_http_address>[^\t]+)\s+'  # 节点HTTP地址
            r'(?P<logUrl>.+)$'  # 日志链接
        )

        for line in output.splitlines():
            if line.startswith("container_"):
                match = pattern.match(line)
                if not match:
                    raise ValueError("无法解析该行数据")

                parsed_data = match.groupdict()
                url = parsed_data['logUrl'].replace("http:", "")
                parsed_data['logUrl'] = "http:{}".format(url)
                if parsed_data['containerId'].encode("utf-8").endswith("000001"):
                    parsed_data['priority'] = "0"
                else:
                    parsed_data['priority'] = "1"

                ret.append(parsed_data)
    elif version == 0 and v == 1:
        pattern = re.compile(
            r'^(?P<containerId>\S+)\s+'  # 容器ID
            r'(?P<start_time>[^\t]+)\s+'  # 启动时间
            r'(?P<allocated_vcores>[^\t]+)\s+'  # 分配的VCores数量
            r'(?P<state>[^\t]+)\s+'  # 状态
            r'(?P<node_rpc_address>[^\t]+)\s+'  # 节点RPC地址
            r'(?P<node_http_address>[^\t]+)\s+'  # 节点HTTP地址
            r'(?P<logUrl>.+)$'  # 日志链接
        )
        for line in output.splitlines():
            if line.startswith("container_"):
                match = pattern.match(line)
                if not match:
                    raise ValueError("无法解析该行数据")

                parsed_data = match.groupdict()
                url = parsed_data['logUrl'].replace("http:", "")
                if parsed_data['containerId'].encode("utf-8").endswith("000001"):
                    parsed_data['priority'] = "0"
                else:
                    parsed_data['priority'] = "1"

                ret.append(parsed_data)

    return ret


def list_yarn_containers(attemptId):
    ret = []
    try:
        proc = subprocess.Popen(['yarn', 'container', '-list', attemptId], stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()

        if proc.returncode == 0:
            ret = parse_yarn_container_list(stdout.decode('utf-8'))
        else:
            print("命令执行失败")
            print("错误信息:")
            print(stderr.decode('utf-8'))
    except OSError as e:
        print("命令执行时发生错误: {}".format(e))
    return ret


def get_attempt_info(type, appId, attemptId,driverLog):
    logset = set([driverLog])
    atts = []

    if version == 0:
        atts = list_yarn_containers(attemptId)
    else:
        url = "{}/ws/v1/cluster/apps/{}/appattempts/{}/containers".format(yarn_url, appId, attemptId)
        response = requests.get(url)
        if response.status_code == 200:
            att_data = response.json()
            atts = att_data['container']
        else:
            print("查询attempts列表失败")


    idx = 0
    dataset = []
    for att in atts:
        cid = att['containerId'].encode('utf-8')
        logs = att['logUrl'].encode('utf-8')
        pri = att['priority']
        logset.discard(logs)
        logarr = []
        if type.lower() == "SPARK".lower():

            logarr.append(logs + "/stderr/")
            logarr.append(logs + "/stdout/")
        else:
            if pri == "1":
                logName = "taskmanager"
            else:
                logName = "jobmanager"

            logarr.append(logs + "/{}.log/".format(logName))
            logarr.append(logs + "/{}.out/".format(logName))
            logarr.append(logs + "/{}.err/".format(logName))

        for log in logarr:
            idx += 1
            tmp = OrderedDict([
                ("id", idx),
                ("containerId", cid),
                ("logs", log),
            ])

            dataset.append(tmp)
    if type.lower() == "SPARK".lower() and len(logset) != 0:
        logs = []
        logs.append(driverLog + "/stderr/")
        logs.append(driverLog + "/stdout/")
        for log in logs:
            idx += 1
            tmp = OrderedDict([
                ("id", idx),
                ("containerId", "deadDriver"),
                ("logs", log),
            ])
            dataset.append(tmp)
    cm.print_dataset("Logs List", dataset)
    return dataset


offset = -4096


def main_loop():
    getConfig()
    total_length = 90
    print("*" * total_length)
    print('' + "yarn 日志查询工具".center(total_length - 3) + '')
    print("*" * total_length)
    print('' + "1. 输入 'exit' 退出程序")
    print('' + "2. 按[B]返回上一级")
    print('' + "3. 参数[verision]: 低版本yarn container不支持rest api, 请将version设置为0")
    print('' + "4. 参数[v]: version=0时生效,yarn container --list 版本字段不同,0:低版本,1:高版本")
    print('' + "5. 日志打印: {id} [offset],offset默认 %d" % offset)
    print('' + "6. 日志存储: {id} s or {id} {offset} s ")
    print("*" * total_length)
    print("*" * total_length)
    print("")
    global context
    global level
    global user_input
    level = 0
    while True:
        try:
            if level != 0:
                user_input = raw_input(context)
                if len(user_input) == 0:
                    continue
            else:
                user_input = user_input
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
            process_command(user_input, back)

        # except EOFError:
        #    print("\n检测到 EOF，正在退出程序...")
        # break
        except KeyboardInterrupt:
            print("\n检测到 Ctrl+C，正在退出程序...")
            break


context = ""
level = 0
user_input = ""
preCmd = [0]


def getParam(ret, index):
    if int(index) <= len(ret) and int(index) > 0:
        return ret[int(index) - 1]
    else:
        print(">> 请输入范围为{}~{}的数字".format(1, len(ret)))


def process_command(command, back):
    global level
    #global ret
    global context
    global offset
    ret = ""
    #    global appId
    #    global type
    # 这里可以添加处理不同命令的逻辑
    split = command.split(" ")
    command = split[0]
    # print("你输入了:level: {}, cmd:{}".format(level,command))
    if level > 0 :
        cur = preCmd[level]
        pre = preCmd[level - 1]
        if back:

            command = cur['cmd']
            ret = pre['ret']
        else:
            ret = pre['ret']

    if level < 0:
        level = 0

    if level == 0:
        ret = get_app_list()
        if len(ret) == 0:
            raw_input("没有运行的程序(随意输入可继续执行)...")
            return
        context = ">> 请输入applicationId序号: "
        preCmd.insert(level, {'cmd': command, 'ret': ret})
        level += 1
    elif level == 1:
        appMap = getParam(ret, command)
        appId = appMap['applicationId'].encode("utf-8")
        type = appMap['type'].encode("utf-8")
        ret = list_app(type, appId)

        context = ">> 请输入attemptId序号: "

        preCmd.insert(level, {'cmd': command, 'ret': ret, 'applicationId': appId, 'type': type})

        level += 1
    elif level == 2:
        aid = getParam(ret, command)
        attemptId = aid['attemptId']
        driverLog = aid['logs']
        pcmd = preCmd[level - 1]
        type = pcmd['type']
        appId = pcmd['applicationId']
        ret = get_attempt_info(type, appId, attemptId,driverLog)
        context = ">> 请输入日志id: "
        preCmd.insert(level, {'cmd': command, 'ret': ret})
        level += 1
    elif level == 3:
        d = getParam(ret, command)
        save = '0'

        if (len(split) == 2):
            if split[1] == 's':
                save = '1'
            else:
                offset = int(split[1])
        elif len(split) == 3:
            offset = int(split[1])
            save = '1'
        output = subprocess.check_output(['python', 'log.py', d['logs'], str(offset), save])
        print("日志开始>>>\n")
        print(output)
        print("日志结束>>>\n")
        cm.print_dataset("Logs List", ret)


    if level != 0 and level != 3:
        print(">> 按[B]返回上一级")


if __name__ == "__main__":
    main_loop()
