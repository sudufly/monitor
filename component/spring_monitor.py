# coding:utf-8
import time
import xml.etree.ElementTree as ET

import requests

from common import common as cm
from component.wx_client import WxClient
from config.config import Config

service = 'Spring应用检测'


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


class SpringMonitor(object):
    config = Config()
    app_set = set()
    instance_map = {}
    wx = WxClient()

    def __init__(self):
        eureka_url = self.config.get_eureka_url()
        self.app_set = self.config.get_spring_app_name_set()

    def get_apps(self, xml_content, filter=True):
        root = ET.fromstring(xml_content)
        app_map = {}
        # 遍历所有 Application 元素
        for app in root.findall('application'):
            app_name = app.find('name').text.encode('utf-8')
            if filter and app_name not in self.app_set:
                continue
            # print('\nApplication: {}'.format(app_name))
            l = list()
            app_map[app_name] = l
            # 遍历每个实例
            tp = None
            for instance in app.findall('instance'):
                instance_id = instance.find('instanceId').text
                status = instance.find('status').text
                tp = (instance_id, status)
                if status == 'UP':
                    app_map[app_name] = (tp)

                # print('  Instance ID: {}, Status: {}'.format(instance_id, status))

                # if status not in ['UP', 'STARTING']:
                #     print('  Warning: Instance {} of application {} is not healthy.'.format(instance_id, app_name))

        return app_map

    def check_eureka_services(self):
        wx = self.wx
        project = self.config.project
        stateMap = self.instance_map
        warning_interval = self.config.get_warning_interval()
        cur_time = time.time()
        eureka_url = '{}/eureka/apps'.format(self.config.get_eureka_url())
        try:
            response = requests.get(eureka_url)
            response.raise_for_status()

            content_type = response.headers.get('Content-Type', '').lower()
            if 'application/xml' not in content_type and 'text/xml' not in content_type:
                print("Response is not in XML format.")
                return

            apps = self.get_apps(response.text)
            # print apps

            for name, ins in apps.items():
                state = ins[1]
                info = stateMap.get(name, {'state': state, 'detectTime': cur_time})
                last_state = info['state']
                if state != 'UP' and last_state != 'UP':
                    # 累计时长
                    if state != last_state:
                        wx.send(change(project, service, name, '状态变化:{} -> {}'.format(last_state, state)))
                        print '{} -> {}'.format(last_state, state)
                elif state == 'UP' and last_state != 'UP':
                    print ("{} 恢复 {} -> {}".format(name, last_state, state))
                    wx.send(
                        recover(project, service, name, info['detectTime'], '状态恢复,{} -> {}'.format(
                            last_state, state)))
                    info['state'] = state
                    info['detectTime'] = cur_time
                    info['alarmTime'] = 0

                elif state != 'UP' and last_state == 'UP':
                    print "异常"
                    wx.send(err(project, service, name, info['detectTime'],
                                '状态异常,{} -> {}'.format(last_state, state)))

                    info['state'] = state
                    info['detectTime'] = cur_time
                    info['alarmTime'] = cur_time

                stateMap[name] = info

            for name in self.app_set:
                name = name.encode('utf-8')
                has = stateMap.has_key(name)
                if has and name in set(apps.keys()):
                    info = stateMap[name]
                    state = info['state']
                    if state != 'UP':
                        alarmTime = info.get('alarmTime', 0)
                        if (cur_time - alarmTime > warning_interval):
                            info['alarmTime'] = cur_time
                            print "{}重复报警,{}".format(name, info)
                            wx.send(con(project, service, name, info['detectTime'],
                                        '异常持续,{} -> {}'.format(last_state, state)))

                    stateMap[name] = info

                else:
                    self.del_exit(name, cur_time)

        except requests.exceptions.RequestException as e:
            name = 'EUREKA'
            self.del_exit(name, cur_time)
            print("An error occurred while fetching data from Eureka: {}".format(e))
        except ET.ParseError as e:
            print("Failed to parse XML: {}".format(e))

    def monitor(self):
        self.check_eureka_services()

    def del_exit(self, name, cur_time):
        stateMap = self.instance_map
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

        eureka_url = '{}/eureka/apps'.format(self.config.get_eureka_url())

        response = requests.get(eureka_url)
        response.raise_for_status()

        content_type = response.headers.get('Content-Type', '').lower()
        if 'application/xml' not in content_type and 'text/xml' not in content_type:
            print("Response is not in XML format.")
            return

        apps = self.get_apps(response.text, False)
        # print response.text
        print ','.join(apps)
        # for app in apps:
        #     print app
