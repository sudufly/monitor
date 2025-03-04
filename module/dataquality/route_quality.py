#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import csv

import psycopg2

from config.config import Config


# 替换MySQL驱动为PostgreSQL驱动
class RouteQuality:
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
        if self.config.get_quality_route_enable():
            parsed_info = self.parse_jdbc_url(self.config.get_db_url())

            # print("Host:", parsed_info['host'])
            # print("Port:", parsed_info['port'])
            # print("Database:", parsed_info['database'])
            self.db = psycopg2.connect(
                host=parsed_info['host'],
                user=self.config.get_db_user(),
                password=self.config.get_db_password(),
                dbname=parsed_info['database'],
                port=int(parsed_info['port'])
            )
            self.cursor = self.db.cursor()

    def get_terminal_to_vin_mapping(self):
        """从 t_car 表获取 terminal_id 与 car_vin 的映射"""
        query = """
            SELECT terminal_id, car_vin 
            FROM t_car
        """
        self.cursor.execute(query)
        return {row[0]: row[1] for row in self.cursor.fetchall()}

    def get_daily_summary(self, date):
        """从日统计表获取数据"""
        query = """
            SELECT dev_id as terminal_id, mileage_pluse as mileage,oil_cost
            FROM t_o_vehicule_day
            WHERE clct_date_ts = %s
        """
        self.cursor.execute(query, (date,))
        return {row[0]: row for row in self.cursor.fetchall()}

    def get_route_sum(self, date):
        """从行程表计算当日里程总和"""
        query = """
            SELECT 
            terminal_id, 
            SUM(mileage) AS route_sum,
            SUM(oil_cost) AS route_oil_sum,
            CASE 
                WHEN MAX(end_time)::date <> MIN(start_time)::date THEN 'Yes'
                ELSE 'No'
            END AS is_cross_day
        FROM t_drive_route
        WHERE (DATE(start_time) = %s OR DATE(end_time) = %s)
        GROUP BY terminal_id
        """
        self.cursor.execute(query, (date, date))
        results = self.cursor.fetchall()
        route_sum = {}
        for row in results:
            terminal_id = row[0]
            route_sum[terminal_id] = {
                'sum_mileage': row[1],
                'sum_oil': row[2],
                'is_cross_day': row[3]
            }

        return route_sum

    def validate(self, date):
        """执行校验逻辑"""
        daily_data = self.get_daily_summary(date)
        route_sum = self.get_route_sum(date)
        terminal_to_vin = self.get_terminal_to_vin_mapping()

        discrepancies = []

        # 检查所有车辆
        all_vehicles = set(daily_data.keys()) | set(route_sum.keys())

        daily_mileage = 0
        daily_oil = 0
        for vid in all_vehicles:
            daily = daily_data.get(vid, None)
            if daily is not None:
                daily_mileage = daily[1]
                daily_oil = daily[2]
            route_data = route_sum.get(vid, {'sum_mileage': 0, 'is_cross_day': 'No', 'sum_oil': 0})
            route_mileage = route_data['sum_mileage']
            route_oil = route_data['sum_oil']
            is_cross_day = route_data['is_cross_day']
            if daily_mileage is None:
                daily_mileage = 0

            if daily_oil is None:
                daily_oil = 0
            if route_mileage is None:
                route_mileage = 0
            if route_oil is None:
                route_oil = 0
            if is_cross_day is None:
                is_cross_day = ''
            # 允许1公里的误差

            diff_mileage = abs(int(daily_mileage) - int(route_mileage))
            diff_oil = abs(int(daily_oil) - int(route_oil))
            if diff_mileage > 1 or diff_oil  > 1:
                discrepancies.append({
                    'car_vin': terminal_to_vin.get(vid, 'Unknown'),
                    'terminal_id': vid,
                    'daily_mileage': daily_mileage,
                    'route_mileage': route_mileage,
                    'diff_mileage': diff_mileage,
                    'daily_oil': daily_oil,
                    'route_oil': route_oil,
                    'diff_oil': diff_oil,
                    'is_cross_day': is_cross_day
                })

        return discrepancies

    def generate_report(self, discrepancies):
        """生成校验报告"""
        if not discrepancies:
            print("✅ All vehicle mileage data matches")
            return

        print("🚨 Found discrepancies:")
        for item in discrepancies:
            print("Vehicle {}:".format(item['car_vin']))
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
            writer.writerow(['vin', '数据号', '日表里程', '分段里程', '里程差', '日表油耗','分段油耗','油耗差','是否跨天'])
            # 写入数据
            for item in discrepancies:
                writer.writerow([item['car_vin'], item['terminal_id'],
                                 item['daily_mileage'], item['route_mileage'], item['diff_mileage'],
                                 item['daily_oil'], item['route_oil'], item['diff_oil'],
                                 item['is_cross_day']])

        print(u"CSV report generated: {}".format(filename))

    def process(self, target_date, dir):
        validator = RouteQuality()
        issues = validator.validate(target_date)
        # validator.generate_report(issues)
        validator.generate_csv_report(issues, u'{}/{}行程统计分析{}.csv'.format(dir,self.config.project.decode('utf-8'), target_date))
