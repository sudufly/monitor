#!/usr/bin/python
# coding:utf-8
import sys
import time
import urlparse
from collections import OrderedDict
from datetime import datetime

import requests

from common import common as cm
from common.common import get_duration
from config.config import Config

# 解决 Python 2 中文乱码问题
reload(sys)
sys.setdefaultencoding('utf-8')

# ==============================================================================
# 配置区域（根据你的环境修改）
# ==============================================================================
# YARN_RM_URL = "http://172.16.2.102:18088"  # YARN ResourceManager 地址
SPARK_APP_NAME = "saas-hq4etl"  # 你的 Spark 应用名称（必须与 YARN 上一致）
CHECK_INTERVAL = 5  # 监控周期（秒），默认 60 秒
DISPLAY_LIMIT = 10  # 每次展示的批次数
VERBOSE_MODE = True  # 是否展示详细日志（True/False）


# ==============================================================================
# 工具函数
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


def extract_spark_ui_base_url(tracking_url):
    """从 trackingUrl 提取 Spark UI 基础地址（用于拼接 API 路径）"""
    if not tracking_url:
        return None
    # 解析 URL：http://hbase2-102:18088/proxy/application_1762061107182_0046/
    parsed = urlparse.urlparse(tracking_url)
    # 拼接基础 URL（保留 scheme、netloc、path，去掉 query 和 fragment）
    # 最终基础 URL：http://hbase2-102:18088/proxy/application_1762061107182_0046
    base_url = urlparse.urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path.rstrip('/'),  # 去掉末尾的 /，避免后续拼接重复
        '', '', ''
    ))
    return base_url


# ==============================================================================
# YARN 与 Spark 交互类
# ==============================================================================
class YarnSparkMonitor(object):
    def __init__(self, spark_app_name):
        config = Config()
        yarn_url = config.yarn_url
        self.yarn_rm_url = yarn_url
        self.spark_app_name = spark_app_name
        self.spark_ui_url = None  # Spark UI 访问地址（如：http://hbase2-102:18088/proxy/application_1762061107182_0046）
        self.spark_api_base_url = None  # Spark UI API 基础地址（如：http://hbase2-102:18088/proxy/application_1762061107182_0046/api/v1）
        self.spark_app_id = None  # Spark 应用 ID（如：application_1762061107182_0046）

    def discover_spark_ui(self):
        """通过 YARN API 自动发现 Spark UI 地址（适配实际返回字段）"""
        log_info("开始在 YARN 上查找应用: {}".format(self.spark_app_name))
        yarn_api = "{}/ws/v1/cluster/apps".format(self.yarn_rm_url)
        params = {
            "applicationTypes": "SPARK",
            "name": self.spark_app_name,
            "states": "RUNNING"
        }

        try:
            response = requests.get(yarn_api, params=params, timeout=15)
            if not validate_response(response):
                return False

            data = response.json()
            apps = data.get("apps", {}).get("app", [])
            if not apps:
                log_error("未找到运行中的 Spark 应用: {}".format(self.spark_app_name))
                return False

            # 处理单个应用的情况（确保是列表）
            if not isinstance(apps, list):
                apps = [apps]

            # 筛选目标应用（匹配名称和运行状态）
            target_app = None
            for app in apps:
                app_name = app.get("name")
                app_state = app.get("state")
                if app_name == self.spark_app_name and app_state == "RUNNING":
                    target_app = app
                    break

            if not target_app:
                log_error("应用状态不是 RUNNING（当前状态：{}）: {}".format(app_state, self.spark_app_name))
                return False

            # 关键修改：从 trackingUrl 提取 Spark UI 地址
            tracking_url = target_app.get("trackingUrl")
            if not tracking_url:
                log_error("YARN 响应中未找到 trackingUrl 字段")
                return False
            log_info("从 YARN 获取到 trackingUrl: {}".format(tracking_url))

            # 提取 Spark UI 基础地址并拼接 API 路径
            self.spark_ui_url = extract_spark_ui_base_url(tracking_url)
            self.spark_api_base_url = "{}/api/v1".format(self.spark_ui_url)
            # 从 trackingUrl 提取 Spark 应用 ID（path 最后一部分）
            self.spark_app_id = self.spark_ui_url.split('/')[-1]

            log_info("成功发现 Spark UI: {}".format(self.spark_ui_url))
            log_info("Spark API 基础地址: {}".format(self.spark_api_base_url))
            log_info("Spark 应用 ID: {}".format(self.spark_app_id))
            return True

        except Exception as e:
            log_error("发现 Spark UI 失败: {}".format(e))
            return False

    def get_batch_summary(self, limit=10):
        """获取批次摘要信息（使用新的 API 基础地址）"""
        if not self.spark_api_base_url or not self.spark_app_id:
            log_error("请先调用 discover_spark_ui 完成初始化")
            return None

        try:
            # 构造批次信息 API 地址（适配代理路径）
            api_url = "{}/applications/{}/streaming/batches".format(
                self.spark_api_base_url,
                self.spark_app_id
            )
            params = {"limit": limit} if limit else None
            # print api_url
            response = requests.get(api_url, params=params, timeout=15)

            if not validate_response(response):
                log_error("获取批次信息失败：请检查 Spark UI 地址是否可访问")
                return None

            batches = response.json()
            log_info("获取到 {} 个批次信息, limit:{}".format(len(batches), limit))
            if len(batches) > limit:
                return batches[:limit]
            return batches

        except Exception as e:
            log_error("获取批次信息失败: {}".format(e))
            return None

    def get_executors_summary(self):
        """获取批次摘要信息（使用新的 API 基础地址）"""
        if not self.spark_api_base_url or not self.spark_app_id:
            log_error("请先调用 discover_spark_ui 完成初始化")
            return None

        try:
            # 构造批次信息 API 地址（适配代理路径）
            api_url = "{}/applications/{}/executors".format(
                self.spark_api_base_url,
                self.spark_app_id
            )
            # print api_url
            response = requests.get(api_url, timeout=15)

            if not validate_response(response):
                log_error("获取批次信息失败：请检查 Spark UI 地址是否可访问")
                return None

            batches = response.json()

            return batches

        except Exception as e:
            log_error("获取批次信息失败: {}".format(e))
            return None

    def display_batch_summary(self, batches):
        """展示批次摘要日志"""
        if not batches:
            log_error("没有批次信息可展示")
            return

        log_info("\n" + "=" * 110)
        log_info("Spark Streaming 批次摘要 (最近 {} 个批次)".format(len(batches)))
        log_info("=" * 110)

        # 表头
        title = "{:<22}| {:<12}| {:<15}| {:<15}| {:<10}| {:<25}|"
        print(title.format(
            "Batch Time", "Input Size", "schedulingDelay", "processingTime", "totalDelay", "Output Ops: Succeeded/Total"
        ))
        print("-" * 110)

        for batch in batches:
            # print(json.dumps(batch, indent=2))

            # batch_id = batch.get("batchId", "N/A")
            batch_id = datetime.fromtimestamp(batch.get("batchId", 0) / 1000).strftime("%Y-%m-%d %H:%M:%S")
            input_size = batch.get("inputSize", "N/A")
            scheduling_delay = batch.get("schedulingDelay", "N/A")
            processing_time = batch.get("processingTime", "N/A")
            total_delay = batch.get("totalDelay", "N/A")
            complete = batch.get("numCompletedOutputOps", 0)
            total = batch.get("numTotalOutputOps", 0)

            # 计算输入消息数
            input_rows = 0
            if "inputSources" in batch:
                for source in batch["inputSources"]:
                    input_rows += source.get("numInputRows", 0)

            print(title.format(
                batch_id, input_size, get_duration(scheduling_delay), get_duration(processing_time),
                get_duration(total_delay), "{}/{}".format(complete, total)
            ))

        log_info("=" * 110 + "\n")

    def display_executors_summary(self, infos):
        """展示摘要日志"""
        if not infos:
            log_error("没有executors信息可展示")
            return
        data = []

        for info in infos:
            isActive = info.get('isActive')
            rddBlocks = info.get('rddBlocks')
            maxMemory = cm.get_size(info.get('maxMemory'))
            memoryUsed = cm.get_size(info.get('memoryUsed'))
            diskUsed = cm.get_size(info.get('diskUsed'))
            totalCores = info.get('totalCores')
            activeTasks = info.get('activeTasks')
            failedTasks = info.get('failedTasks')
            completeTasks = info.get('completedTasks')
            totalDuration = cm.get_duration(info.get('totalDuration'))
            totalGCTime = cm.get_duration(info.get('totalGCTime'))
            totalShuffleRead = cm.get_size(info.get('totalShuffleRead'))
            totalShuffleWrite = cm.get_size(info.get('totalShuffleWrite'))

            data.append(OrderedDict([
                ("ExecutorId", info.get('id')),
                ("Address", info.get('hostPort')),
                ("Status", 'Active' if isActive else 'Dead'),
                ("RDD Blocks", rddBlocks),
                ("Storage Memory", "{}/{}".format(memoryUsed, maxMemory)),
                ("Disk Used", diskUsed),
                ("Cores", totalCores),
                ("Active Tasks", activeTasks),
                ("Failed Tasks", failedTasks),
                ("Complete Tasks", completeTasks),
                # ("total Tasks", "{}/{}".format(memoryUsed, maxMemory)),
                ("Task Time/GC Time", "{}/{}".format(totalDuration, totalGCTime)),
                ("Shuffle Read", totalShuffleRead),
                ("Shuffle Write", totalShuffleWrite),

            ]))
        cm.print_dataset('Summary', data)

        # log_info("=" * 110 + "\n")


# ==============================================================================
# 主监控逻辑
# ==============================================================================
def main():
    sys.argv[0]
    monitor = YarnSparkMonitor(
        spark_app_name=sys.argv[1]
    )
    info_type = 'batch'
    if len(sys.argv) >= 2:
        info_type = sys.argv[2]

    # 初始化连接（关键：自动发现 Spark UI 地址）
    log_info("初始化监控器...")
    if not monitor.discover_spark_ui():
        log_error("初始化失败，退出程序")
        sys.exit(1)

    # 开始循环监控
    log_info("监控器启动成功，开始监控 (周期: {}s)".format(CHECK_INTERVAL))
    try:
        while True:
            # batches = monitor.get_batch_summary(limit=DISPLAY_LIMIT)
            # batches = monitor.get_executors_summary(limit=DISPLAY_LIMIT)
            # if batches:
            if info_type == 'batch':
                batches = monitor.get_batch_summary(limit=DISPLAY_LIMIT)
                monitor.display_batch_summary(batches)
            elif info_type == 'executor':
                batches = monitor.get_executors_summary()
                monitor.display_executors_summary(batches)
            # else:
            #     log_error("获取信息失败，{} 秒后重试".format(CHECK_INTERVAL))

            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        log_info("监控器被手动停止")
    except Exception as e:
        log_error("监控器异常: {}".format(e))
    finally:
        log_info("监控器退出")


if __name__ == "__main__":
    main()
