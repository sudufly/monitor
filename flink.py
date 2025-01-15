# coding:utf-8
import sys
from collections import OrderedDict

from common import common as cm
from component.yarn_tool import YarnTool


if __name__ == "__main__":


    applicationId = 'application_1735628121942_0006'
    if len(sys.argv) > 1:
        applicationId = sys.argv[1]
    yarn = YarnTool()
    taskmanagers = yarn.get_taskmanagers(applicationId)
    for taskmanager in taskmanagers:
        memory = []
        tid = taskmanager['id']
        r = yarn.get_taskmanagers_view(applicationId, tid)
        m = yarn.get_taskmanagers_metrics(applicationId, tid)
        configed_mem = r['memoryConfiguration']
        metrics = r['metrics']
        free = r['freeResource']
        # for k, v in configed_mem.items():
        # cfg = memory.get('Framework Heap', {})
        # memory['Framework Heap'] = {'Effective Configuration': int(configed_mem.get('frameworkHeap')) / 1024 / 1024}
        memory.append(OrderedDict([
            ("Flink Memory Model", 'Total Flink Memory'),
            ("Effective Configuration", '{}'.format(cm.get_size(configed_mem.get('totalFlinkMemory')))),
            ("Used", '-'),
        ]))
        memory.append(OrderedDict([
            ("Flink Memory Model", 'Total Process Memory'),
            ("Effective Configuration", '{}'.format(cm.get_size(configed_mem.get('totalProcessMemory')))),
            ("Used", '-'),
        ]))
        memory.append(OrderedDict([
            ("Flink Memory Model", 'Framework Heap'),
            ("Effective Configuration", '{}'.format(cm.get_size(configed_mem.get('frameworkHeap')))),
            ("Used", '{} (Framework + Task)'.format(cm.get_size(metrics['heapUsed']))),
        ]))
        memory.append(OrderedDict([
            ("Flink Memory Model", 'Task Heap'),
            ("Effective Configuration", '{}'.format(cm.get_size(configed_mem['taskHeap']))),
            ("Used", '{} (Framework + Task)'.format(cm.get_size(metrics['heapUsed']))),
        ]))
        memory.append(OrderedDict([
            ("Flink Memory Model", 'Managed Memory'),
            ("Effective Configuration", '{}'.format(cm.get_size(configed_mem['managedMemory']))),
            ("Used", '{}'.format(cm.get_size(free['managedMemory']))),
        ]))
        memory.append(OrderedDict([
            ("Flink Memory Model", 'Framework Off-Heap'),
            ("Effective Configuration", '{}'.format(cm.get_size(configed_mem['frameworkOffHeap']))),
            ("Used", '-'),
        ]))
        memory.append(OrderedDict([
            ("Flink Memory Model", 'Task Off-Heap'),
            ("Effective Configuration", '{}'.format(cm.get_size(configed_mem['taskOffHeap']))),
            ("Used", '-'),
        ]))
        memory.append(OrderedDict([
            ("Flink Memory Model", 'Network'),
            ("Effective Configuration", '{}'.format(cm.get_size(configed_mem['networkMemory']))),

            ("Used", '{} KB'.format(cm.get_size(metrics['nettyShuffleMemoryUsed']))),

        ]))
        id = 'Status.JVM.Memory.Metaspace.Used'
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

        cm.print_dataset('{} Memory'.format(tid), memory)
