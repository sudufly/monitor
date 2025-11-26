#!/usr/bin/python
# coding:utf-8
import signal
import sys
import time
import traceback
import schedule
from datetime import datetime

import pytz

from common import common as cm
from component.kafka_infra_monitor import MonitorKafkaInfra
from component.spring_monitor import SpringMonitor
from component.wx_client import WxClient
from component.yarn_app_monitor import YarnAppMonitor
from config.config import Config

from qualityTools import Report

tz = pytz.timezone('Asia/Shanghai')
service = "告警监控服务"

def is_run_crossing_midnight(start_time, end_time):
    """判断程序运行是否跨天"""
    return start_time.date() != end_time.date()

gmap = {}

def signal_handler(sig, monitor):
    print('你按下了 Ctrl+C! 或者程序收到了 SIGTERM')
    # 在这里加入你的清理代码
    gmap['wx'].send(stop_markdown(gmap['project'], service, gmap['starttime'], 'RECEIVE SIG {}'.format(sig)), True)
    print("监控服务退出 RECEIVE SIG{}".format(sig))
    sys.exit(0)

# 设置信号处理器
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class Monitor(object):
    config = Config()
    project = ''
    wx = WxClient()
    kafka_monitor = MonitorKafkaInfra()
    yarn_app_monitor = YarnAppMonitor()
    spring_monitor = SpringMonitor()
    starttime = time.time() * 1000

    monitor_funcs = []

    def __init__(self):
        global gmap
        # self.config = cm.getConfig()
        self.project = self.config.project
        gmap = {'wx': self.wx, 'project': self.project, 'starttime': self.starttime}

    def get_monitor_item(self):
        index = 1
        arr = []
        ye = self.config.yarn_enable
        if ye:
            arr.append('{}. 监控Yarn应用程序'.format(index))
            self.monitor_funcs.append(self.yarn_app_monitor.monitor)
            index += 1

        ke = self.config.kafka_enable
        if ke:
            arr.append('{}. 监控Kafka消费组'.format(index))
            self.monitor_funcs.append(self.kafka_monitor.monitor)
            index += 1
        se = self.config.spring_enable

        if se:
            arr.append('{}. 监控Spring应用程序'.format(index))
            self.monitor_funcs.append(self.spring_monitor.monitor)
            index += 1
        return arr

    def monitor(self):

        try:
            for m in self.monitor_funcs:
                m()
        except Exception as e:
            print("loop err")
            traceback.print_exc()
            self.wx.send(err_markdown(self.project, service, e), True)
    def daily_report(self):


        report = Report()
        report.report()
    def start(self):
        starttime = self.starttime
        print("监控服务启动成功")
        items = self.get_monitor_item()

        self.wx.send(start_markdown(self.project, service, starttime, '\n'.join(items)), True)

        schedule.every().day.at("00:00:00").do(self.send_heart_beat)
        schedule.every().day.at("06:00:00").do(self.daily_report)
        #每5秒
        schedule.every(5).seconds.do(self.monitor)

        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except Exception as e:
            traceback.print_exc()
            self.wx.send(stop_markdown(self.project, service, starttime, e), True)
            print("监控服务退出:{}".format(e))
        except KeyboardInterrupt as e:
            traceback.print_exc()
            self.wx.send(stop_markdown(self.project, service, starttime, e), True)
            print("监控服务退出:{}".format(e))

    def send_heart_beat(self):
        self.wx.send(heart_markdown(self.project, service, self.starttime), True)

def heart_markdown(project, service, starttime):
    markdown = """
<font color = info >{project}告警服务 [心跳]</font>
><font color = info >启动时间:</font>  {starttime} 
><font color = info >当前时间:</font>  {timestamp} 
    """
    return markdown.format(
        project=project,
        service=service,
        starttime=cm.utc_ms_to_time(starttime),
        timestamp=cm.get_time()
    )

def start_markdown(project, service, starttime, m):
    markdown = """
<font color = info >{project}告警服务 [启动]</font>
><font color = info >启动时间:</font>  {timestamp} 
><font color = info >服务说明:</font>  \n{m}
"""
    return markdown.format(
        project=project,
        service=service,
        m=m,
        timestamp=cm.utc_ms_to_time(starttime)
    )

def stop_markdown(project, service, starttime, context):
    markdown = """
<font color = warning >{project}告警服务 [异常]</font>
><font color = info >启动时间:</font>  {starttime} 
><font color = info >退出时间:</font>  {timestamp} 
><font color = info >退出原因:</font>  {context} 
    """
    return markdown.format(
        project=project,
        service=service,
        context=context,
        starttime=cm.utc_ms_to_time(starttime),
        timestamp=cm.get_time()
    )

def err_markdown(project, service, context):
    markdown = """
<font color = warning >{project}告警服务 [异常]</font>
><font color = info >触发时间:</font>  {timestamp} 
><font color = info >异常:</font>  {context} 
    """
    return markdown.format(
        project=project,
        service=service,
        context=context,
        timestamp=cm.get_time()
    )

if __name__ == "__main__":
    monitor = Monitor()
    monitor.start()
