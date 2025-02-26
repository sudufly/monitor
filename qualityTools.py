#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import csv
import os

import psycopg2

from config.config import Config


# 替换MySQL驱动为PostgreSQL驱动
class MileageValidator:
    config = Config()

    def parse_jdbc_url(self, jdbc_url):
        # 去掉前缀 jdbc:postgresql://
        jdbc_url = jdbc_url[len("jdbc:postgresql://"):]

        # 分割 host:port 和 database
        host_port, database = jdbc_url.split('/', 1)

        # 分割 host 和 port
        host, port = host_port.split(':', 1)

        return {
            'host': host,
            'port': int(port),
            'database': database
        }

    def __init__(self):
        parsed_info = self.parse_jdbc_url(self.config.get_db_url())

        print("Host:", parsed_info['host'])
        print("Port:", parsed_info['port'])
        print("Database:", parsed_info['database'])
        self.db = psycopg2.connect(
            host=parsed_info['host'],
            user=self.config.get_db_user(),
            password=self.config.get_db_password(),
            dbname=parsed_info['database'],
            port=int(parsed_info['port'])
        )
        self.cursor = self.db.cursor()

    def get_daily_summary(self, date):
        """从日统计表获取数据"""
        query = """
            SELECT dev_id as terminal_id, mileage 
            FROM t_o_vehicule_day
            WHERE clct_date_ts = %s
        """
        self.cursor.execute(query, (date,))
        return {row[0]: row[1] for row in self.cursor.fetchall()}

    def get_route_sum(self, date):
        """从行程表计算当日里程总和"""
        query = """
            SELECT terminal_id, SUM(mileage) 
            FROM t_drive_route
            WHERE DATE(start_time) = %s
            GROUP BY terminal_id
        """
        self.cursor.execute(query, (date,))
        return {row[0]: row[1] for row in self.cursor.fetchall()}

    def validate(self, date):
        """执行校验逻辑"""
        daily_data = self.get_daily_summary(date)
        route_sum = self.get_route_sum(date)

        discrepancies = []

        # 检查所有车辆
        all_vehicles = set(daily_data.keys()) | set(route_sum.keys())

        for vid in all_vehicles:
            daily_mileage = daily_data.get(vid, 0)
            route_total = route_sum.get(vid, 0)
            if daily_mileage is None:
                daily_mileage = 0
            if route_total is None:
                route_total = 0

            # 允许1公里的误差
            if abs(int(daily_mileage) - int(route_total)) > 1:
                discrepancies.append({
                    'terminal_id': vid,
                    'daily': daily_mileage,
                    'route_sum': route_total,
                    'diff': abs(daily_mileage - route_total)
                })

        return discrepancies

    def generate_report(self, discrepancies):
        """生成校验报告"""
        if not discrepancies:
            print("✅ All vehicle mileage data matches")
            return

        print("🚨 Found discrepancies:")
        for item in discrepancies:
            print("Vehicle {}:".format(item['terminal_id']))
            print("  Daily report: {}km".format(item['daily']))
            print("  Route sum: {}km".format(item['route_sum']))
            print("  Difference: {}km\n".format(item['diff']))

    def generate_csv_report(self, discrepancies, filename):
        """生成CSV校验报告"""
        if not discrepancies:
            print("✅ No discrepancies to report")
            return

        with open(filename, mode='w') as file:
            writer = csv.writer(file)
            # 写入表头
            writer.writerow(['terminal_id', 'daily', 'route_sum', 'diff'])
            # 写入数据
            for item in discrepancies:
                writer.writerow([item['terminal_id'], item['daily'], item['route_sum'], item['diff']])

        print("CSV report generated: {}".format(filename))


if __name__ == "__main__":
    dir = "./report"
    if not os.path.exists(dir):
        os.makedirs(dir)
    validator = MileageValidator()
    target_date = "2023-12-01"  # 可改为动态获取日期
    issues = validator.validate(target_date)
    # validator.generate_report(issues)
    validator.generate_csv_report(issues, './report/' + target_date + '.csv')
