#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import os
import sys

import pandas as pd
import psycopg2
from openpyxl.chart import BarChart, Reference

from config.config import Config


class FuelElectricConsumption:
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

        self.db = psycopg2.connect(
            host=parsed_info['host'],
            user=self.config.get_db_user(),
            password=self.config.get_db_password(),
            dbname=parsed_info['database'],
            port=int(parsed_info['port'])
        )
        self.cursor = self.db.cursor()

    def get_fuel_consumption(self, date):
        query = """
                WITH RECURSIVE DESCENDANTS AS (
		select uo.*,'' as modelParentStr  from
		t_car_model uo
		where uo.model_level = (
		    select max(model_level) from t_car_model

		)
		UNION ALL
		SELECT  B.* ,concat_ws(' ',D.modelParentStr,D.model_name) as modelParentStr
		FROM t_car_model B
		INNER JOIN DESCENDANTS D ON D.model_id = B.model_parent
		)
		,m as(
		select
model_id,energy_type,concat_ws(' ',modelParentStr,model_name) as model_name
from
DESCENDANTS
		)
        SELECT 
            clct_date_ts::date AS clct_date,
            car_model_id,
            tcm.model_name,
            SUM(oil_cost) / count(1) AS avg_oil_cost,
            SUM(mileage) / count(1) AS avg_mileage,
            SUM(engine_time) / count(1) AS avg_engine_time,
            count(1) as online_cnt
        FROM 
            t_o_vehicule_day td
        JOIN 
            t_car tc ON td.dev_id = tc.terminal_id
        JOIN 
            m tcm ON tc.car_model_id = tcm.model_id
        WHERE 
            tcm.energy_type = 1 AND clct_date_ts::date = %s AND time_zone =8 and online_state = 1
        GROUP BY 
            clct_date_ts::date, car_model_id, tcm.model_name;
        """

        self.cursor.execute(query, (date,))
        df = pd.DataFrame(self.cursor.fetchall(),
                          columns=['clct_date', 'car_model_id', 'model_name', 'avg_oil_cost', 'avg_mileage',
                                   'avg_engine_time','online_cnt'])

        df['avg_oil_cost'] = df['avg_oil_cost'].astype(float)
        df['avg_mileage'] = df['avg_mileage'].astype(float)
        df['avg_engine_time'] = df['avg_engine_time'].astype(float)/3600.0
        df['avg_oil_consumption_per_hour'] = df.apply(
            lambda row: row['avg_oil_cost'] / (row['avg_engine_time'] ) if row[
                                                                                           'avg_engine_time'] != 0 else 0,
            axis=1)

        return df[['clct_date', 'car_model_id', 'model_name', 'avg_oil_cost', 'avg_mileage', 'avg_engine_time',
                   'avg_oil_consumption_per_hour','online_cnt']]

    def get_electric_consumption(self, date):
        query = """
                WITH RECURSIVE DESCENDANTS AS (
		select uo.*,'' as modelParentStr  from
		t_car_model uo
		where uo.model_level = (
		    select max(model_level) from t_car_model

		)
		UNION ALL
		SELECT  B.* ,concat_ws(' ',D.modelParentStr,D.model_name) as modelParentStr
		FROM t_car_model B
		INNER JOIN DESCENDANTS D ON D.model_id = B.model_parent
		)
		,m as(
		select
model_id,energy_type,concat_ws(' ',modelParentStr,model_name) as model_name
from
DESCENDANTS
		)
        SELECT 
            clct_date_ts::date AS clct_date,
            car_model_id,
            tcm.model_name,
            SUM(power_cost) / count(1) AS total_power_cost,
            SUM(mileage) / count(1) AS avg_mileage,
            SUM(engine_time) / count(1) AS avg_engine_time,
            count(1) as online_cnt
        FROM 
            t_o_vehicule_day td
        JOIN 
            t_car tc ON td.dev_id = tc.terminal_id
        JOIN 
            m tcm ON tc.car_model_id = tcm.model_id
        WHERE 
            tcm.energy_type = 2 AND clct_date_ts::date = %s AND time_zone =8 and online_state = 1
        GROUP BY 
            clct_date_ts::date, car_model_id, tcm.model_name;
        """
        self.cursor.execute(query, (date,))
        df = pd.DataFrame(self.cursor.fetchall(),
                          columns=['clct_date', 'car_model_id', 'model_name', 'total_power_cost', 'avg_mileage',
                                   'avg_engine_time','online_cnt'])
        if len(df) == 0:
            return df
        df['total_power_cost'] = df['total_power_cost'].astype(float)
        df['avg_mileage'] = df['avg_mileage'].astype(float)
        df['avg_engine_time'] = df['avg_engine_time'].astype(float)/3600.0
        df['avg_power_consumption_per_hour'] = df.apply(
            lambda row: row['total_power_cost'] / (row['avg_engine_time'] ) if row[
                                                                                             'avg_engine_time'] != 0 else 0,
            axis=1)
        return df[['clct_date', 'car_model_id', 'model_name', 'total_power_cost', 'avg_mileage', 'avg_engine_time',
                   'avg_power_consumption_per_hour','online_cnt']]

    def get_fuel_detail(self, date):
        query = """
                WITH RECURSIVE DESCENDANTS AS (
		select uo.*,'' as modelParentStr  from
		t_car_model uo
		where uo.model_level = (
		    select max(model_level) from t_car_model

		)
		UNION ALL
		SELECT  B.* ,concat_ws(' ',D.modelParentStr,D.model_name) as modelParentStr
		FROM t_car_model B
		INNER JOIN DESCENDANTS D ON D.model_id = B.model_parent
		)
		,m as(
		select
model_id,energy_type,concat_ws(' ',modelParentStr,model_name) as model_name
from
DESCENDANTS
		)
        SELECT 
            clct_date_ts::date AS clct_date,
            car_model_id,
            tcm.model_name,
            oil_cost,
            mileage,
            engine_time
        FROM 
            t_o_vehicule_day td
        JOIN 
            t_car tc ON td.dev_id = tc.terminal_id
        JOIN 
            m tcm ON tc.car_model_id = tcm.model_id
        WHERE 
            tcm.energy_type = 1 AND clct_date_ts::date = %s AND time_zone =8 and online_state = 1;
        """
        self.cursor.execute(query, (date,))
        df = pd.DataFrame(self.cursor.fetchall(),
                          columns=['clct_date', 'car_model_id', 'model_name', 'oil_cost', 'mileage', 'engine_time'])
        if len(df) == 0:
            return df
        df['oil_cost'] = df['oil_cost'].astype(float)
        df['mileage'] = df['mileage'].astype(float)
        df['engine_time'] = df['engine_time'].astype(float)/3600.0
        df['oil_consumption_per_hour'] = df.apply(
            lambda row: row['oil_cost'] / (row['engine_time'] ) if row['engine_time'] != 0 else 0, axis=1)
        return df

    def get_electric_detail(self, date):
        query = """
        WITH RECURSIVE DESCENDANTS AS (
		select uo.*,'' as modelParentStr  from
		t_car_model uo
		where uo.model_level = (
		    select max(model_level) from t_car_model

		)
		UNION ALL
		SELECT  B.* ,concat_ws(' ',D.modelParentStr,D.model_name) as modelParentStr
		FROM t_car_model B
		INNER JOIN DESCENDANTS D ON D.model_id = B.model_parent
		)
		,m as(
		select
        model_id,energy_type,concat_ws(' ',modelParentStr,model_name) as model_name
        from
        DESCENDANTS
		)
        
        SELECT 
            clct_date_ts::date AS clct_date,
            car_model_id,
            tcm.model_name,
            power_cost,
            mileage,
            engine_time
        FROM 
            t_o_vehicule_day td
        JOIN 
            t_car tc ON td.dev_id = tc.terminal_id
        JOIN 
            m tcm ON tc.car_model_id = tcm.model_id
        WHERE 
            tcm.energy_type = 2 AND clct_date_ts::date = %s AND time_zone =8 and online_state = 1 ;
        """
        self.cursor.execute(query, (date,))
        df = pd.DataFrame(self.cursor.fetchall(),
                          columns=['clct_date', 'car_model_id', 'model_name', 'power_cost', 'mileage', 'engine_time'])
        if len(df) == 0:
            return df
        df['power_cost'] = df['power_cost'].astype(float)
        df['mileage'] = df['mileage'].astype(float)
        df['engine_time'] = df['engine_time'].astype(float)/3600.0
        df['power_consumption_per_hour'] = df.apply(
            lambda row: row['power_cost'] / (row['engine_time'] ) if row['engine_time'] != 0 else 0, axis=1)
        return df

    def write_to_excel(self, dfs, filename):

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            fuel = u'传统车'
            elec = u'新能源'
            fuel_detail = u'传统车明细'
            elec_detail = u'新能源明细'
            dfs['fuel_avg'].to_excel(writer, sheet_name=fuel, index=False)
            dfs['electric_avg'].to_excel(writer, sheet_name=elec, index=False)
            dfs['fuel_detail'].to_excel(writer, sheet_name=fuel_detail, index=False)
            dfs['electric_detail'].to_excel(writer, sheet_name=elec_detail, index=False)

            # 添加柱状图到 Fuel Avg Consumption
            ws_fuel_avg = writer.sheets[fuel]



            self.add_bar_chart(u"平均油耗/小时", dfs['fuel_avg'], ws_fuel_avg,7, "K2")
            self.add_bar_chart(u"平均油耗", dfs['fuel_avg'], ws_fuel_avg, 4,'K17')
            self.add_bar_chart(u"平均里程", dfs['fuel_avg'], ws_fuel_avg, 5,'K31')
            self.add_bar_chart(u"平均时长", dfs['fuel_avg'], ws_fuel_avg, 6,'K45')


            # 添加柱状图到 Electric Avg Consumption
            ws_electric_avg = writer.sheets[elec_detail]


            self.add_bar_chart(u"平均电耗/小时", dfs['electric_avg'], ws_electric_avg,7, "K2")
            self.add_bar_chart(u"平均电耗", dfs['electric_avg'], ws_electric_avg, 4,'K16')
            self.add_bar_chart(u"平均里程", dfs['electric_avg'], ws_electric_avg, 5,'K31')
            self.add_bar_chart(u"平均时长", dfs['electric_avg'], ws_electric_avg, 6,'K45')

        print("Excel report generated: {}".format(filename))

    def add_bar_chart(self, title, dfs, sheet, c1,column):
        chart = BarChart()
        chart.title = title
        chart.height = 6
        datas = Reference(sheet, min_col=c1, min_row=1,
                                              max_row=len(dfs) + 1)
        labels = Reference(sheet, min_col=3, min_row=2,
                                                max_row=len(dfs) + 1)
        chart.add_data(datas, titles_from_data=True)
        chart.set_categories(labels)

        # chart_electric_engine_time.dataLabels = True
        sheet.add_chart(chart, column)

    def generate_reports(self, date):
        dir = "./report"
        if not os.path.exists(dir):
            os.makedirs(dir)

        fuel_avg_df = self.get_fuel_consumption(date)
        electric_avg_df = self.get_electric_consumption(date)
        fuel_detail_df = self.get_fuel_detail(date)
        electric_detail_df = self.get_electric_detail(date)

        dfs = {
            'fuel_avg': fuel_avg_df,
            'electric_avg': electric_avg_df,
            'fuel_detail': fuel_detail_df,
            'electric_detail': electric_detail_df
        }

        self.write_to_excel(dfs, './report/consumption_report_{}.xlsx'.format(date.replace('-', '')))


if __name__ == "__main__":
    target_date = None
    if len(sys.argv) >= 2:
        target_date = sys.argv[1]
    else:
        print("Usage: python2 daily_quality.py <date>")
        sys.exit(1)

    validator = FuelElectricConsumption()
    validator.generate_reports(target_date)
