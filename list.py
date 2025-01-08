# coding:utf-8
from component.spring_monitor import SpringMonitor
from component.yarn_app_monitor import YarnAppMonitor

if __name__ == "__main__":
    spring = SpringMonitor()
    yarn = YarnAppMonitor()
    yarn.get_app_list()

    spring.list()
