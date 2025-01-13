# coding:utf-8
import sys
from collections import OrderedDict

import requests

from common import common as cm

FLINK_REST_API_HOST = 'http://hbase2-102:18088'


def get_taskmanagers(flink_rest_api_host='http://localhost:8081'):
    url = "{}/taskmanagers".format(flink_rest_api_host)
    response = requests.get(url)
    print url

    if response.status_code == 200:
        return response.json().get('taskmanagers', [])
    else:
        print("Failed to retrieve task managers, status code: {}".format(response.status_code))
        return []


def get_taskmanagers_view(flink_rest_api_host, tid):
    url = "{}/taskmanagers/{}".format(flink_rest_api_host, tid)
    response = requests.get(url)
    print url

    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to retrieve task managers, status code: {}".format(response.status_code))
        return []


def get_taskmanagers_metrics(url, tid):
    metrics = [
        'Status.JVM.Memory.Heap.Used', 'Status.JVM.Memory.Heap.Max', 'Status.Shuffle.Netty.UsedMemory',
        'Status.Shuffle.Netty.TotalMemory', 'Status.Flink.Memory.Managed.Used', 'Status.Flink.Memory.Managed.Total',
        'Status.JVM.Memory.Metaspace.Used', 'Status.JVM.Memory.Metaspace.Max'

    ]
    mset = set(metrics)
    url = '{}/taskmanagers/{}/metrics?get={}'.format(
        url, tid, ','.join(metrics))
    response = requests.get(url)

    print url
    print response.text
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


if __name__ == "__main__":
    burl = 'http://hbase2-102:34139'
    burl = 'http://hbase2-102:18088/proxy/application_1735628121942_0006'
    if len(sys.argv) > 1:
        burl = sys.argv[0]

    taskmanagers = get_taskmanagers(burl)
    for taskmanager in taskmanagers:
        memory = []
        tid = taskmanager['id']
        r = get_taskmanagers_view(burl, tid)
        m = get_taskmanagers_metrics(burl, tid)
        configed_mem = r['memoryConfiguration']
        metrics = r['metrics']
        free = r['freeResource']
        print "configed"
        # for k, v in configed_mem.items():
        # cfg = memory.get('Framework Heap', {})
        # memory['Framework Heap'] = {'Effective Configuration': int(configed_mem.get('frameworkHeap')) / 1024 / 1024}
        memory.append(OrderedDict([
            ("Flink Memory Model", 'Total Flink Memory'),
            ("Effective Configuration", '{} MB'.format(int(configed_mem.get('totalFlinkMemory')) / 1024 / 1024)),
            ("Used", '-'),
        ]))
        memory.append(OrderedDict([
            ("Flink Memory Model", 'Total Process Memory'),
            ("Effective Configuration", '{} MB'.format(int(configed_mem.get('totalProcessMemory')) / 1024 / 1024)),
            ("Used", '-'),
        ]))
        memory.append(OrderedDict([
            ("Flink Memory Model", 'Framework Heap'),
            ("Effective Configuration", '{} MB'.format(int(configed_mem.get('frameworkHeap')) / 1024 / 1024)),
            ("Used", '{} MB (Framework + Task)'.format(metrics['heapUsed'] / 1024 / 1024)),
        ]))
        memory.append(OrderedDict([
            ("Flink Memory Model", 'Task Heap'),
            ("Effective Configuration", '{} MB'.format(configed_mem['taskHeap'] / 1024 / 1024)),
            ("Used", '{} MB (Framework + Task)'.format(metrics['heapUsed'] / 1024 / 1024)),
        ]))
        memory.append(OrderedDict([
            ("Flink Memory Model", 'Managed Memory'),
            ("Effective Configuration", '{} MB'.format(configed_mem['managedMemory'] / 1024 / 1024)),
            ("Used", '{} MB'.format(free['managedMemory'] / 1024 / 1024)),
        ]))
        memory.append(OrderedDict([
            ("Flink Memory Model", 'Framework Off-Heap'),
            ("Effective Configuration", '{} MB'.format(configed_mem['frameworkOffHeap'] / 1024 / 1024)),
            ("Used", '-'),
        ]))
        memory.append(OrderedDict([
            ("Flink Memory Model", 'Task Off-Heap'),
            ("Effective Configuration", '{} MB'.format(configed_mem['taskOffHeap'] / 1024 / 1024)),
            ("Used", '-'),
        ]))
        memory.append(OrderedDict([
            ("Flink Memory Model", 'Network'),
            ("Effective Configuration", '{} MB'.format(configed_mem['networkMemory'] / 1024 / 1024)),

            ("Used", '{} KB'.format(metrics['nettyShuffleMemoryUsed'] / 1 / 1024)),

        ]))
        id = 'Status.JVM.Memory.Metaspace.Used'
        print m[id]
        memory.append(OrderedDict([
            ("Flink Memory Model", 'JVM Metaspace'),
            ("Effective Configuration", '{} MB'.format(configed_mem['jvmMetaspace'] / 1024 / 1024)),
            ("Used", '{} MB'.format(format(int(m[id]['value']) / 1024 / 1024))),
        ]))
        memory.append(OrderedDict([
            ("Flink Memory Model", 'JVM Overhead'),
            ("Effective Configuration", '{} MB'.format(configed_mem['jvmOverhead'] / 1024 / 1024)),
            ("Used", '-'),
        ]))
        # print ('key:{},value:{}M'.format(k, v / 1024 / 1024))
        # print
        # print
        # 'userd\n'
        # for k, v in metrics.items():
        #     if
        # type(v) == int:
        # print ('key:{},value:{}M'.format(k, v / 1024 / 1024))
        #
        # # print ret
        cm.print_dataset('{} Memory'.format(tid), memory)
