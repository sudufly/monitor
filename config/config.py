# coding:utf-8
from common import common as cm


class Config:
    config = None
    project = ''
    bootstrap_servers = ''
    warning_offsets = 100000
    warning_interval = 600
    # 不监控该消费组
    black_groupid_set = set()
    yarn_url = ''
    yarn_app_name_set = set()
    kafka_enable = True
    yarn_enable = True
    spring_enable = True
    eureka_url = ''
    spring_app_name_set = set()

    db_url = ""
    db_user = ""
    db_password = ""

    def __init__(self, debug=False):

        self.config = cm.getConfig(debug)
        yarn = self.config['yarn']
        self.yarn_url = yarn['rm_url']
        self.project = self.config['project']['project'].encode('utf-8')
        self.bootstrap_servers = self.config['kafka']['bootstrapServers'].encode('utf-8')
        monitor = self.config['monitor']

        self.warning_offsets = int(monitor['warningOffsets'])
        self.warning_interval = int(monitor['warnInterval'])
        ids = monitor['blackGroupIds']
        if len(ids) > 0:
            self.black_groupid_set = set(ids.split(','))
            self.black_groupid_set.discard('')

        names = monitor['yarnAppNames'].encode('utf-8')

        if len(names) > 0:
            self.yarn_app_name_set = set(names.split(','))
            self.yarn_app_name_set.discard('')

        if 'kafkaEnable' in monitor:
            self.kafka_enable = monitor['kafkaEnable'].lower() == 'True'.lower()
        if 'yarnEnable' in monitor:
            self.yarn_enable = monitor['yarnEnable'].lower() == 'True'.lower()

        self.eureka_url = monitor.get('springEnable')
        self.eureka_url = monitor.get('eurekaUrl')
        spring_app_names = monitor.get('springAppNames', '').encode('utf-8')
        self.spring_enable = monitor.getboolean('springEnable')
        if len(spring_app_names) > 0:
            self.spring_app_name_set = set(spring_app_names.split(','))
            self.spring_app_name_set.discard('')

        db = self.config['db']
        if db is not None:

            self.db_url = db.get('url','')
            self.db_user = db.get('user','')
            self.db_password = db.get('password','')

    def get_config(self):
        return self.config

    def get_yarn_url(self):
        return self.yarn_url

    def get_project(self):
        return self.project

    def get_bootstrap_servers(self):
        return self.bootstrap_servers

    def get_warning_offsets(self):
        return self.warning_offsets

    def get_warning_interval(self):
        return self.warning_interval

    def get_black_groupid_set(self):
        return self.black_groupid_set

    def get_yarn_app_name_set(self):
        return self.yarn_app_name_set

    def get_kafka_enable(self):
        return self.kafka_enable

    def get_yarn_enable(self):
        return self.yarn_enable

    def get_eureka_url(self):
        return self.eureka_url

    def get_spring_app_name_set(self):
        return self.spring_app_name_set

    def get_spring_enable(self):
        return self.spring_enable

    def get_db_url(self):
        return self.db_url
    def get_db_user(self):
        return self.db_user
    def get_db_password(self):
        return self.db_password