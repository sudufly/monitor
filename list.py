#!/usr/bin/python
# coding:utf-8
from component.spring_monitor import SpringMonitor
from component.yarn_app_monitor import YarnAppMonitor

if __name__ == "__main__":
    spring = SpringMonitor()
    yarn = YarnAppMonitor()
    print("Yarn List")
    yarn.list()
    print ("")
    print("Spring List")
    spring.list()
