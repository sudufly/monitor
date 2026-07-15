#!/usr/bin/python
# coding:utf-8
import sys
import time
import json
from collections import OrderedDict
from datetime import datetime

import requests

# 解决 Python 2 中文乱码问题
reload(sys)
sys.setdefaultencoding('utf-8')

# ==============================================================================
# 配置区域（HBase 1.x适配）
# ==============================================================================
HBASE_MASTER_URL = "http://172.16.2.102:60010"  # HBase Master地址（1.x默认端口60010）
CHECK_INTERVAL = 5  # 监控周期（秒）
DISPLAY_LIMIT = 10  # 展示的RegionServer数量（0表示全部）

# HBase 1.x标准JMX Bean名称（关键适配点）
JMX_BEAN_MASTER = "hadoop:service=HBase,name=Master,sub=Server"
JMX_BEAN_RS_SERVER = "hadoop:service=HBase,name=RegionServer,sub=Server"
JMX_BEAN_RS_STORAGE = "hadoop:service=HBase,name=RegionServer,sub=Storage"

# ==============================================================================
# 工具函数（兼容原逻辑，适配HBase 1.x）
# ==============================================================================
def log_info(message):
    """打印带时间戳的信息日志"""
    print("[{}] [INFO] {}".format(
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        message
    ))


def log_error(message):
    """打印带时间戳的错误日志"""
    print("[{}] [ERROR] {}".format(
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        message
    ))


def validate_response(response):
    """验证 HTTP 响应是否正常"""
    try:
        response.raise_for_status()
        return True
    except requests.exceptions.HTTPError as e:
        log_error("HTTP 请求失败: {} - {}".format(response.status_code, e))
        return False
    except Exception as e:
        log_error("请求异常: {}".format(e))
        return False


def get_size(bytes_size, decimal=2):
    """字节数格式化（转KB/MB/GB/TB）"""
    if bytes_size is None or bytes_size == "N/A" or bytes_size == 0:
        return "N/A"
    bytes_size = float(bytes_size)
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    unit_index = 0
    while bytes_size >= 1024 and unit_index < len(units) - 1:
        bytes_size /= 1024
        unit_index += 1
    return "{:.{}f} {}".format(bytes_size, decimal, units[unit_index])


def get_duration(ms, decimal=2):
    """毫秒数格式化（转秒/分/时）"""
    if ms is None or ms == "N/A" or ms == 0:
        return "N/A"
    ms = float(ms)
    if ms < 1000:
        return "{:.{}f} ms".format(ms, decimal)
    seconds = ms / 1000
    if seconds < 60:
        return "{:.{}f} s".format(seconds, decimal)
    minutes = seconds / 60
    if minutes < 60:
        return "{:.{}f} min".format(minutes, decimal)
    hours = minutes / 60
    return "{:.{}f} h".format(hours, decimal)


def get_rate(value_diff, time_diff, unit=""):
    """计算速率（值差/时间差）"""
    if value_diff is None or time_diff <= 0 or value_diff < 0:
        return "N/A"
    rate = float(value_diff) / time_diff
    if unit in ["B", "KB", "MB", "GB"]:
        return get_size(rate) + "/s"
    return "{:.2f} {}".format(rate, unit + "/s")


# ==============================================================================
# HBase 1.x 监控核心类（无rs-status接口适配）
# ==============================================================================
class HBase1xClusterMonitor(object):
    def __init__(self, hbase_master_url):
        self.hbase_master_url = hbase_master_url.rstrip('/')
        self.rs_metrics_cache = {}  # 缓存RS指标用于速率计算
        self.last_collect_time = None  # 上一次采集时间

    def get_jmx_metrics(self, bean_name=None):
        """
        HBase 1.x JMX API适配：路径为/jmx/json，支持按bean名称过滤
        :param bean_name: JMX Bean名称（如hadoop:service=HBase,name=Master,sub=Server）
        :return: JMX指标列表
        """
        # HBase 1.x标准JMX路径是 /jmx/json（关键区别于2.x的/jmx）
        jmx_api = "{}/jmx/json".format(self.hbase_master_url)
        print jmx_api
        params = {}
        if bean_name:
            params["qry"] = bean_name

        try:
            response = requests.get(jmx_api, params=params, timeout=15)
            print response
            if not validate_response(response):
                return None

            data = response.json()
            beans = data.get("beans", [])
            if not beans:
                log_error("JMX响应为空（bean: {}）".format(bean_name or "all"))
                return None
            return beans
        except Exception as e:
            log_error("获取JMX指标失败: {} - {}".format(bean_name, e))
            return None

    def get_region_servers(self):
        """
        替代rs-status接口：从Master的JMX指标提取在线RegionServer列表
        HBase 1.x Master的JMX中，RegionServers字段是逗号分隔的RS地址（如host1:60020,host2:60020）
        """
        master_beans = self.get_jmx_metrics(JMX_BEAN_MASTER)
        if not master_beans:
            return None

        master_metric = master_beans[0]
        rs_list_str = master_metric.get("RegionServers", "")
        if not rs_list_str:
            log_error("Master JMX中未找到RegionServers字段")
            return None

        # 解析RS列表（格式：host1:60020,host2:60020）
        rs_list = [rs.strip() for rs in rs_list_str.split(",") if rs.strip()]
        online_rs = [{"name": rs, "status": "ONLINE"} for rs in rs_list]
        log_info("从Master JMX发现 {} 个在线RegionServer".format(len(online_rs)))
        return online_rs

    def collect_rs_metrics(self):
        """采集HBase 1.x RegionServer内存、读写核心指标"""
        # 1. 获取在线RS列表
        rs_list = self.get_region_servers()
        if not rs_list:
            return None

        # 2. 获取所有RS的Server级指标（内存、Region数等）
        rs_server_beans = self.get_jmx_metrics(JMX_BEAN_RS_SERVER)
        if not rs_server_beans:
            return None

        # 3. 获取所有RS的Storage级指标（读写请求、字节数等）
        rs_storage_beans = self.get_jmx_metrics(JMX_BEAN_RS_STORAGE)
        if not rs_storage_beans:
            return None

        # 4. 初始化采集时间和时间差
        current_time = time.time()
        self.last_collect_time = self.last_collect_time or current_time
        time_diff = current_time - self.last_collect_time

        # 5. 构建Storage指标映射（按RS主机名分组）
        storage_mapping = {}
        for bean in rs_storage_beans:
            # HBase 1.x JMX的name字段格式：hadoop:service=HBase,name=RegionServer,sub=Storage,server=host1:60020
            name_str = bean.get("name", "")
            # 提取RS主机名（从server=host1:60020中解析）
            rs_host = ""
            for part in name_str.split(","):
                if part.strip().startswith("server="):
                    rs_host = part.strip().split("=")[1].split(":")[0]  # 只取主机名，去掉端口
                    break
            if not rs_host:
                continue

            # HBase 1.x Storage核心指标
            storage_mapping[rs_host] = {
                "readRequestsCount": bean.get("readRequestsCount", 0),
                "writeRequestsCount": bean.get("writeRequestsCount", 0),
                "totalReadBytes": bean.get("totalReadBytes", 0),
                "totalWriteBytes": bean.get("totalWriteBytes", 0),
                "blockCacheSize": bean.get("blockCacheSize", 0),
                "blockCacheUsed": bean.get("blockCacheUsed", 0)
            }

        # 6. 整合Server和Storage指标
        rs_metrics = {}
        for bean in rs_server_beans:
            # 提取RS主机名
            name_str = bean.get("name", "")
            rs_host = ""
            for part in name_str.split(","):
                if part.strip().startswith("server="):
                    rs_host = part.strip().split("=")[1].split(":")[0]
                    break
            if not rs_host or rs_host not in storage_mapping:
                continue

            # HBase 1.x Server级内存指标
            heap_used_mb = bean.get("heapMemoryUsedMB", 0)
            heap_max_mb = bean.get("heapMemoryMaxMB", 0)
            non_heap_used_mb = bean.get("nonHeapMemoryUsedMB", 0)
            non_heap_max_mb = bean.get("nonHeapMemoryMaxMB", 0)
            region_count = bean.get("regions", 0)

            # 读取Storage指标
            storage = storage_mapping[rs_host]
            read_reqs = storage["readRequestsCount"]
            write_reqs = storage["writeRequestsCount"]
            read_bytes = storage["totalReadBytes"]
            write_bytes = storage["totalWriteBytes"]
            block_cache_size = storage["blockCacheSize"]
            block_cache_used = storage["blockCacheUsed"]

            # 计算实时速率（对比缓存）
            cache = self.rs_metrics_cache.get(rs_host, {})
            read_reqs_diff = read_reqs - cache.get("readRequestsCount", 0)
            write_reqs_diff = write_reqs - cache.get("writeRequestsCount", 0)
            read_bytes_diff = read_bytes - cache.get("totalReadBytes", 0)
            write_bytes_diff = write_bytes - cache.get("totalWriteBytes", 0)

            read_reqs_rate = get_rate(read_reqs_diff, time_diff, "req")
            write_reqs_rate = get_rate(write_reqs_diff, time_diff, "req")
            read_bytes_rate = get_rate(read_bytes_diff, time_diff, "B")
            write_bytes_rate = get_rate(write_bytes_diff, time_diff, "B")

            # 更新缓存
            self.rs_metrics_cache[rs_host] = {
                "readRequestsCount": read_reqs,
                "writeRequestsCount": write_reqs,
                "totalReadBytes": read_bytes,
                "totalWriteBytes": write_bytes,
                "timestamp": current_time
            }

            # 组装最终指标
            rs_metrics[rs_host] = {
                "host": rs_host,
                "heap_memory": "{:.2f}MB/{:.2f}MB".format(heap_used_mb, heap_max_mb),
                "non_heap_memory": "{:.2f}MB/{:.2f}MB".format(non_heap_used_mb, non_heap_max_mb),
                "block_cache": "{} used/{} total".format(get_size(block_cache_used), get_size(block_cache_size)),
                "read_reqs_total": read_reqs,
                "write_reqs_total": write_reqs,
                "read_reqs_rate": read_reqs_rate,
                "write_reqs_rate": write_reqs_rate,
                "read_bytes_total": get_size(read_bytes),
                "write_bytes_total": get_size(write_bytes),
                "read_bytes_rate": read_bytes_rate,
                "write_bytes_rate": write_bytes_rate,
                "region_count": region_count
            }

        # 更新采集时间
        self.last_collect_time = current_time
        return rs_metrics

    def get_cluster_summary(self):
        """获取HBase 1.x集群汇总指标"""
        master_beans = self.get_jmx_metrics(JMX_BEAN_MASTER)
        if not master_beans:
            return None

        master_metric = master_beans[0]
        # 解析在线RS数量
        rs_list_str = master_metric.get("RegionServers", "")
        online_rs_count = len([rs for rs in rs_list_str.split(",") if rs.strip()])

        summary = {
            "master_status": "Active" if master_metric.get("ActiveMaster", False) else "Standby",
            "online_rs_count": online_rs_count,
            "total_regions": master_metric.get("numRegions", 0),
            "total_tables": master_metric.get("numTables", 0),
            "master_heap_used": "{:.2f}MB/{:.2f}MB".format(
                master_metric.get("heapMemoryUsedMB", 0),
                master_metric.get("heapMemoryMaxMB", 0)
            ),
            "start_time": datetime.fromtimestamp(master_metric.get("startTime", 0)/1000).strftime("%Y-%m-%d %H:%M:%S")
        }
        return summary

    def display_cluster_summary(self):
        """展示集群汇总信息"""
        summary = self.get_cluster_summary()
        if not summary:
            log_error("无法获取集群汇总指标")
            return

        print("\n" + "=" * 80)
        log_info("HBase 1.x 集群汇总信息")
        print("=" * 80)
        for key, value in summary.items():
            print("{:<20}: {}".format(key.replace("_", " ").title(), value))
        print("=" * 80 + "\n")

    def display_rs_metrics(self, rs_metrics):
        """表格化展示RegionServer指标"""
        if not rs_metrics:
            log_error("无RegionServer指标可展示")
            return

        # 筛选展示数量
        rs_list = list(rs_metrics.values())
        if DISPLAY_LIMIT > 0 and len(rs_list) > DISPLAY_LIMIT:
            rs_list = rs_list[:DISPLAY_LIMIT]

        # 构建展示数据
        data = []
        for rs in rs_list:
            data.append(OrderedDict([
                ("RegionServer", rs["host"]),
                ("Heap Memory", rs["heap_memory"]),
                ("Non-Heap Memory", rs["non_heap_memory"]),
                ("Block Cache", rs["block_cache"]),
                ("Regions", rs["region_count"]),
                ("Read Reqs (total/rate)", "{}/{}".format(rs["read_reqs_total"], rs["read_reqs_rate"])),
                ("Write Reqs (total/rate)", "{}/{}".format(rs["write_reqs_total"], rs["write_reqs_rate"])),
                ("Read Bytes (total/rate)", "{}/{}".format(rs["read_bytes_total"], rs["read_bytes_rate"])),
                ("Write Bytes (total/rate)", "{}/{}".format(rs["write_bytes_total"], rs["write_bytes_rate"]))
            ]))

        # 打印表格
        print("\n" + "=" * 160)
        log_info("HBase 1.x RegionServer 监控指标 (展示前 {} 个节点)".format(len(rs_list)))
        print("=" * 160)
        # 表头
        headers = data[0].keys()
        header_format = "| ".join(["{:<20}"] * len(headers))
        print(header_format.format(*headers))
        print("-" * 160)
        # 数据行
        row_format = "| ".join(["{:<20}"] * len(headers))
        for row in data:
            print(row_format.format(*row.values()))
        print("=" * 160 + "\n")


# ==============================================================================
# 主程序逻辑
# ==============================================================================
def main():
    # 解析命令行参数（支持自定义Master地址）
    hbase_master_url = HBASE_MASTER_URL
    if len(sys.argv) > 1:
        hbase_master_url = sys.argv[1]
        log_info("使用自定义HBase Master地址: {}".format(hbase_master_url))

    # 初始化监控器
    log_info("初始化HBase 1.x集群监控器...")
    monitor = HBase1xClusterMonitor(hbase_master_url)

    # 验证Master连接（先获取RS列表）
    if not monitor.get_region_servers():
        log_error("无法连接到HBase 1.x Master或未发现RegionServer")
        sys.exit(1)

    # 循环监控
    log_info("监控器启动成功，监控周期: {}秒 (按Ctrl+C停止)".format(CHECK_INTERVAL))
    try:
        while True:
            # 采集并展示指标
            rs_metrics = monitor.collect_rs_metrics()
            if rs_metrics:
                monitor.display_cluster_summary()
                monitor.display_rs_metrics(rs_metrics)
            else:
                log_error("指标采集失败，{}秒后重试".format(CHECK_INTERVAL))

            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        log_info("监控器被手动停止")
    except Exception as e:
        log_error("监控器异常退出: {}".format(e))
    finally:
        log_info("HBase 1.x监控器退出")


if __name__ == "__main__":
    main()