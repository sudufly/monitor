#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import os
import sys
import zipfile

import requests

from common import common as cm
from component.email_client import EmailClient
from component.wx_client import WxClient  # 确保 WxClient 类可用
from config.config import Config
from module.dataquality.daily_quality import DailyQuality
from module.dataquality.route_quality import RouteQuality


class Report:
    config = Config()
    route_enable = config.quality_route_enable
    daily_enable = config.quality_daily_enable
    wx = WxClient()  # 初始化 WxClient

    def report(self):
        target_date = cm.get_yesterday_date()
        the_day_before_yesterday = cm.get_the_day_before_yesterday_date()
        dir = "./report/{}".format(target_date)
        if not os.path.exists(dir):
            os.makedirs(dir)

        if sys.argv is not None and len(sys.argv) >= 2:
            target_date = sys.argv[1]

        route_quality = RouteQuality()
        daily_quality = DailyQuality()

        if self.route_enable:
            route_quality.process(the_day_before_yesterday, dir)
        if self.daily_enable:
            daily_quality.process(target_date, dir)

        # 生成压缩包
        zip_path = self.zip_directory(dir, target_date)
        # 发送压缩包到企业微信机器人
        self.send_zip_to_server(zip_path)

    def zip_directory(self, directory, target_date):
        zip_path = "./report/{}.zip".format(target_date)
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(directory):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, start=directory)
                    zipf.write(file_path, arcname)
        return zip_path



    def send_zip_to_server(self, zip_path):
        i = 0
        # email = EmailClient()
        # email.send_zip_via_email(zip_path)


if __name__ == "__main__":
    config = Config()

    target_date = cm.get_yesterday_date()
    if len(sys.argv) >= 2:
        target_date = sys.argv[1]

    # validator = FuelElectricConsumption()
    # validator.generate_reports(target_date)
    dir = "./report/{}".format(target_date)
    if not os.path.exists(dir):
        os.makedirs(dir)
    target_date = cm.get_yesterday_date()

    if sys.argv is not None and len(sys.argv) >= 2:
        target_date = sys.argv[1]

    route_quality = RouteQuality()
    daily_quality = DailyQuality()
    if config.get_quality_route_enable():
        route_quality.process(target_date, dir)
    daily_quality.process(target_date, dir)

    # report = Report()
    # # 生成压缩包
    # zip_path = report.zip_directory(dir, target_date)
    # # 上传压缩包
    # # report.upload_file(zip_path)
    # email = EmailClient()
    # email.send_zip_via_email(zip_path)
