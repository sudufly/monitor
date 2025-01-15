# coding:utf-8
import re
import subprocess
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
from collections import OrderedDict

import requests

from common import common as cm
from config.config import Config


class YarnTool:
    config = Config()
    yarn_url = config.yarn_url

    ycfg = config.config['yarn']
    yarn_url = ycfg['rm_url']
    version = int(ycfg['version'])
    v = int(ycfg['v'])

    def __init__(self):
        print

    def get_app_list(self, types=''):
        """获取所有应用程序的详细信息"""
        # response = requests.get("{}/ws/v1/cluster/apps".format(yarn_url))
        url = "{}/ws/v1/cluster/apps?state=SUBMITTED, ACCEPTED, RUNNING".format(self.yarn_url)
        response = requests.get(url)
        dataset = []
        # print(url)
        type_set = set()
        if len(types) > 0:
            type_set = set(types.split(","))

        if response.status_code == 200:
            apps_data = response.json()
            if apps_data['apps'] is None:
                return dataset
            applications = apps_data['apps']['app']

            idx = 0

            for app in applications:
                type = app['applicationType'].encode('utf-8')
                if len(type_set) > 0 and type not in type_set:
                    continue
                idx = idx + 1
                id = app['id']

                name = app['name'].encode('utf-8')
                progress = app['progress']
                utc_ms = app['startedTime']
                state = app['state'].encode('utf-8')

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

        else:
            print("查询应用列表失败")
        return dataset

    def get_app_attempts(self, appid):
        url = "{}/ws/v1/cluster/apps/{}/appattempts".format(self.yarn_url, appid)
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
                    ("logs", 'http:{}'.format(logs.replace('http:', ''))),
                ])
                dataset.append(tmp)

                # print(fmt % (idx, attemptId, host, logs))
            cm.print_dataset("Attempts List", dataset)

        else:
            print("查询应用列表失败")
        return dataset

    def get_attempt_info(self, type, appId, attemptId, driverLog):
        logset = set([driverLog])
        atts = []

        if self.version == 0:
            atts = self.list_yarn_containers(attemptId)
        else:
            url = "{}/ws/v1/cluster/apps/{}/appattempts/{}/containers".format(self.yarn_url, appId, attemptId)
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

    def list_yarn_containers(self, attemptId):
        ret = []
        try:
            proc = subprocess.Popen(['yarn', 'container', '-list', attemptId], stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            stdout, stderr = proc.communicate()

            if proc.returncode == 0:
                ret = self.parse_yarn_container_list(stdout.decode('utf-8'))
            else:
                print("命令执行失败")
                print("错误信息:")
                print(stderr.decode('utf-8'))
        except OSError as e:
            print("命令执行时发生错误: {}".format(e))
        return ret

    def parse_yarn_container_list(self, output):
        ret = []
        version = self.version
        v = self.v
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

        def get_attempt_info(self, type, appId, attemptId, driverLog):
            logset = set([driverLog])
            atts = []

            if version == 0:
                atts = self.list_yarn_containers(attemptId)
            else:
                url = "{}/ws/v1/cluster/apps/{}/appattempts/{}/containers".format(self.yarn_url, appId, attemptId)
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

    ##################flink#####################

    def get_job_overview(self, applicationId):
        """
            获取flink 任务预览
        """
        yarn_url = self.yarn_url
        """获取所有作业的概述"""
        url = "{}/proxy/{}/jobs/overview".format(yarn_url, applicationId)
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print("Failed to fetch job overview, status code: {}".format(response.status_code))
            return None

    def get_job_details(self, applicationId, job_id):
        """根据job_id获取特定作业的详细信息"""
        url = "{}/proxy/{}/jobs/{}".format(self.yarn_url, applicationId, job_id)
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print("Failed to fetch job details for job {}, status code: {}".format(job_id, response.status_code))
            return None

    def get_checkpoint_data(self, applicationId, job_id):
        """根据job_id获取特定作业的checkpoint数据"""
        url = "{}/proxy/{}/jobs/{}/checkpoints".format(self.yarn_url, applicationId, job_id)
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print("Failed to fetch checkpoint data for job {}, status code: {}".format(job_id, response.status_code))
            return None

    def get_checkpoint_details(self, applicationId, job_id, checkpointId):
        """根据job_id获取特定作业的checkpoint数据"""
        url = "{}/proxy/{}/jobs/{}/checkpoints/details/{}".format(self.yarn_url, applicationId, job_id, checkpointId)
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print("Failed to fetch checkpoint data for job {}, status code: {}".format(job_id, response.status_code))
            return None

    def get_taskmanagers(self, applicationId):
        url = "{}/proxy/{}/taskmanagers".format(self.yarn_url, applicationId)
        response = requests.get(url)
        print url

        if response.status_code == 200:
            return response.json().get('taskmanagers', [])
        else:
            print("Failed to retrieve task managers, status code: {}".format(response.status_code))
            return []

    def get_taskmanagers_view(self, applicationId, tid):
        url = "{}/proxy/{}/taskmanagers/{}".format(self.yarn_url, applicationId, tid)
        response = requests.get(url)

        if response.status_code == 200:
            return response.json()
        else:
            print("Failed to retrieve task managers, status code: {}".format(response.status_code))
            return []

    def get_taskmanagers_metrics(self, applicationId, tid):
        metrics = [
            'Status.JVM.Memory.Heap.Used', 'Status.JVM.Memory.Heap.Max', 'Status.Shuffle.Netty.UsedMemory',
            'Status.Shuffle.Netty.TotalMemory', 'Status.Flink.Memory.Managed.Used', 'Status.Flink.Memory.Managed.Total',
            'Status.JVM.Memory.Metaspace.Used', 'Status.JVM.Memory.Metaspace.Max'

        ]
        mset = set(metrics)
        url = '{}/proxy/{}/taskmanagers/{}/metrics?get={}'.format(
            self.yarn_url, applicationId, tid, ','.join(metrics))
        response = requests.get(url)

        ret = {}
        if response.status_code == 200:
            items = response.json();
            for item in items:
                id = item['id'].encode("utf-8")
                value = item['value'].encode("utf-8")
                if id in mset:
                    ret[id] = {'id': id, 'value': value}

            return ret
        else:
            print("Failed to retrieve task managers, status code: {}".format(response.status_code))
            return {}
