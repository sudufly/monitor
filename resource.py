#!/usr/bin/python
 #coding:utf-8
import sys
from collections import OrderedDict

import requests
import json

from common import common as cm

# Yarn ResourceManager的地址和端口
yarn_url = "http://10.13.2.9:5004"


# 获取集群信息
def get_cluster_info():
    try:
        response = requests.get("{}/ws/v1/cluster/metrics".format(yarn_url))
        if response.status_code == 200:
            data = json.loads(response.text)
            return data["clusterMetrics"]
        else:
            print("信息获取Error:%d " % response.status_code)
            sys.exit(1)
    except Exception as e:
        print("连接Exception: {}".format(e))
        sys.exit(1)
 
# 解析并打印集群信息
def print_cluster_info(cluster_info):
    # 解析参数，并进行单位转换
    total_memory = round( cluster_info["totalMB"]/1024         )
    available_memory = round( cluster_info["availableMB"]/1024 )
    allocated_memory = round( cluster_info["allocatedMB"]/1024 )
    reserved_memory = round( cluster_info["reservedMB"]/1024   )

    reservedVirtualCores = cluster_info['reservedVirtualCores']   
    availableVirtualCores = cluster_info['availableVirtualCores']   
    allocatedVirtualCores = cluster_info['allocatedVirtualCores']   
    containersAllocated = cluster_info['containersAllocated']   
    print("------------- 当前集群内存资源情况-----------------")
    # 打印集群内存资源情况
    print("可用VCores  : %d" % availableVirtualCores)
    print("已分配VCores: %d" % allocatedVirtualCores)
    print("预留VCores  : %d" % reservedVirtualCores)
    print("")
    print("总内存      : %d GB" % total_memory)

    print("可用内存    : %d GB" % available_memory)
    print("已分配内存  : %d GB" % allocated_memory)
    print("预留内存    : % GB" % reserved_memory)
    
    # 检测资源是否已经到位，最低限30T
    if total_memory >= 30 :
        print("资源充足")
    else:
        print("资源扩充可能不充足，立即联系owner")

def get_node_info():
    dataset = []
    try:
        url = "{}/ws/v1/cluster/nodes".format(yarn_url)
        response = requests.get(url)
        # print url
        if response.status_code == 200:
            data = json.loads(response.text)
	#    print response.text.encode("utf-8")
            nodes = data['nodes']['node']
            for node in nodes:
                host = node['nodeHostName'].encode("utf-8")
                containers = node['numContainers']
                state = node['state']
                useMem = node['usedMemoryMB']
                availMemoryMB = node['availMemoryMB']
                usedVirtualCores = node['usedVirtualCores']
                aVcores = node['availableVirtualCores']
                dataset.append(OrderedDict([
                    ("Host", host),
                    ("State", state),
                    ("Containers", containers),
                    ("Available MemoryMB", availMemoryMB),
                    ("Used MemoryMB", useMem),
                    ("Used VCores", usedVirtualCores),
                    ("Available VCores", aVcores),
                ]))

        else:
            print("信息获取Error:%d " % response.status_code)
            sys.exit(1)

    except Exception as e:
        print("连接Exception: {}".format(e))
        sys.exit(1)
    return dataset


def print_node_info(nodeInfo): 
    print("------------- 当前集群节点资源情况-----------------")
    nodes = nodeInfo['nodes']['node']
    fmt = "%-20s|%-15s|%-15s|%-20s|%-15s|%-20s|"
    print fmt % ("Host","Containers","Total Mem MB","Available Mem MB","Total VCores","Available VCores")
    for node in nodes:
        host = node['nodeHostName'].encode("utf-8")
        containers = node['numContainers']
        useMem = node['usedMemoryMB']
        aMem = node['availMemoryMB']
        usedVcores = node['usedVirtualCores']
        aVcores = node['availableVirtualCores']
        print fmt % (host,containers,useMem,aMem,usedVcores,aVcores)
 
# 脚本调用
if __name__ == "__main__":
    config = cm.getConfig()
    yarn_url = config['yarn']['rm_url']
    cluster_info = get_cluster_info()
    print_cluster_info(cluster_info)
    nodes = get_node_info()

    cm.print_dataset("Cluster Nodes List",nodes)
    # print_node_info(nodes)

