# coding:utf-8
import time

import requests

from common import common as cm
from component.wx_client import WxClient
from config.config import Config

service = 'Yarn应用检测'


def err(project, service, app, alarmTime, content):
    markdown = """
<font color = warning >{project}告警 [告警]</font>
><font color = info >服务:</font>  {service} 
><font color = info >应用:</font>  {app} 
><font color = info >触发时间:</font>  {alarmtime} 
><font color = info >报警时间:</font>  {timestamp} 
><font color = info >报警内容:</font>  {content} 
"""

    return markdown.format(
        project=project,
        service=service,
        app=app,
        content=content,
        alarmtime=cm.utc_ms_to_time(alarmTime * 1000),
        timestamp=cm.get_time()
    )


def change(project, service, app, content):
    markdown = """
<font color = warning >{project}告警 [告警]</font>
><font color = info >服务:</font>  {service} 
><font color = info >应用:</font>  {app} 
><font color = info >报警时间:</font>  {timestamp} 
><font color = info >报警内容:</font>  {content} 
"""

    return markdown.format(
        project=project,
        service=service,
        app=app,
        content=content,
        timestamp=cm.get_time()
    )


def con(project, service, app, alarmTime, content):
    markdown = """
<font color = warning >{project}告警 [告警]</font>
><font color = info >服务:</font>  {service} 
><font color = info >应用:</font>  {app} 
><font color = info >触发时间:</font>  {alarmtime} 
><font color = info >报警时间:</font>  {timestamp} 
><font color = info >报警内容:</font>  {content} 
"""

    return markdown.format(
        project=project,
        service=service,
        app=app,
        content=content,
        alarmtime=cm.utc_ms_to_time(alarmTime * 1000),
        timestamp=cm.get_time()
    )


def recover(project, service, app, alarmtime, content):
    markdown = """
<font color = warning >{project}告警 </font><font color = info >[恢复]</font>
><font color = info >服务:</font>  {service} 
><font color = info >应用:</font>  {app} 
><font color = info >异常时间:</font>  {alarmtime} 
><font color = info >恢复时间:</font>  {timestamp} 
><font color = info >报警状态:</font>  {content} 
"""
    return markdown.format(
        project=project,
        service=service,
        app=app,
        content=content,
        alarmtime=cm.utc_ms_to_time(alarmtime * 1000),
        timestamp=cm.get_time()
    )


class YarnAppMonitor:
    config = Config()
    yarn_url = ''
    stataMap = {}
    wx = WxClient()

    def __init__(self):
        self.yarn_url = self.config.yarn_url

    def monitor(self):
        self.check_state()

    def check_state(self):
        stateMap = self.stataMap
        nameset = self.config.get_yarn_app_name_set()
        config = self.config
        project = config.get_project()
        warning_interval = config.get_warning_interval()
        wx = self.wx
        """获取所有应用程序的详细信息"""
        # response = requests.get("{}/ws/v1/cluster/apps".format(yarn_url))
        url = "{}/ws/v1/cluster/apps?state=SUBMITTED, ACCEPTED, RUNNING".format(self.yarn_url)

        cur_time = time.time()

        try:
            response = requests.get(url)
            if response.status_code == 200:
                names = set()
                apps_data = response.json()
                if apps_data['apps'] is None:
                    return
                applications = apps_data['apps']['app']
                for app in applications:
                    name = app['name'].encode('utf-8')
                    state = app['state'].encode('utf-8')

                    if name in nameset:
                        names.add(name)
                        info = stateMap.get(name, {'state': state, 'detectTime': cur_time})
                        last_state = info['state']
                        if state != 'RUNNING' and last_state != 'RUNNING':
                            # 累计时长
                            if state != last_state:
                                wx.send(change(project, service, name, '状态变化:{} -> {}'.format(last_state, state)))
                                print '{} -> {}'.format(last_state, state)
                        elif state == 'RUNNING' and last_state != 'RUNNING':
                            print "恢复"
                            wx.send(
                                recover(project, service, name, info['alarmTime'], '状态恢复,{} -> {}'.format(
                                    last_state, state)))
                            info['state'] = state
                            info['detectTime'] = cur_time
                            info['alarmTime'] = 0

                        elif state != 'RUNNING' and last_state == 'RUNNING':
                            print "异常"
                            wx.send(err(config.get_project(), service, name, info['detectTime'],
                                        '状态异常,{} -> {}'.format(last_state, state)))

                            info['state'] = state
                            info['detectTime'] = cur_time
                            info['alarmTime'] = cur_time
                        elif state == 'RUNNING':
                            info['state'] = state
                            info['detectTime'] = cur_time
                        stateMap[name] = info

                for name in nameset:
                    has = stateMap.has_key(name)
                    if has and name in names:
                        info = stateMap[name]
                        state = info['state']
                        if state != 'RUNNING':
                            alarmTime = info.get('alarmTime', 0)
                            if (cur_time - alarmTime > warning_interval):
                                info['alarmTime'] = cur_time
                                print "{}重复报警,{}".format(name, info)
                                wx.send(con(config.get_project(), service, name, info['detectTime'],
                                            '异常持续,{} -> {}'.format(last_state, state)))

                        stateMap[name] = info


                    else:
                        self.del_exit(name, cur_time)


            else:
                print("查询应用列表失败")
        except:
            self.del_exit('Yarn ResourceManager', cur_time)

    def del_exit(self, name, cur_time):
        stateMap = self.stataMap
        config = self.config
        wx = self.wx
        warning_interval = config.get_warning_interval()

        info = stateMap.get(name, {'state': 'EXIT', 'detectTime': cur_time, 'alarmTime': 0})
        alarmTime = info.get('alarmTime', 0)

        # 退出10min 报一次
        if (cur_time - alarmTime > warning_interval * 3):
            info['alarmTime'] = cur_time
            print "{}异常退出".format(name)
            wx.send(err(config.get_project(), service, name, info['detectTime'],
                        '程序退出,{} -> EXIT'.format(info['state'])))
        info['state'] = 'EXIT'
        stateMap[name] = info

    def list(self):

        """获取所有应用程序的详细信息"""
        # response = requests.get("{}/ws/v1/cluster/apps".format(yarn_url))
        url = "{}/ws/v1/cluster/apps?state=SUBMITTED, ACCEPTED, RUNNING".format(self.yarn_url)

        response = requests.get(url)
        if response.status_code == 200:
            names = set()
            apps_data = response.json()
            if apps_data['apps'] is None:
                return
            applications = apps_data['apps']['app']
            for app in applications:
                name = app['name'].encode('utf-8')
                names.add(name)

            print ','.join(names)
