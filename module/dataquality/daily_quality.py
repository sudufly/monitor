#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from decimal import Decimal, ROUND_DOWN

import pandas as pd
import psycopg2
from openpyxl.chart import BarChart, Reference

from common import common as cm
from component.wx_client import WxClient
from config.config import Config


class DailyQuality:
    service = '日统计检测服务'
    config = Config()
    enable_fuel = config.get_quality_fuel_enable()
    enable_elec = config.get_quality_elec_enable()
    enable_mix = False
    wx = WxClient()
    threshold_dec = config.get_quality_threshold_dec()
    threshold_inc = config.get_quality_threshold_inc()

    cond = '(1 = 1)'
    if config.get_quality_has_online_state():
        cond = '(online_state = 1 or online_state is null)'

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
        if self.config.quality_daily_enable:
            parsed_info = self.parse_jdbc_url(self.config.get_db_url())

            self.db = psycopg2.connect(
                host=parsed_info['host'],
                user=self.config.get_db_user(),
                password=self.config.get_db_password(),
                dbname=parsed_info['database'],
                port=int(parsed_info['port'])
            )
            self.cursor = self.db.cursor()

    common_sql = """
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
		"""

    def get_fuel_consumption(self, date):
        query = self.common_sql + """
                
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
            tcm.energy_type = 1 AND clct_date_ts = %s AND time_zone =8 and {}
        GROUP BY 
            clct_date_ts::date, car_model_id, tcm.model_name;
        """.format(self.cond)

        self.cursor.execute(query, (date,))
        df = pd.DataFrame(self.cursor.fetchall(),
                          columns=['clct_date', 'car_model_id', 'model_name', 'avg_oil_cost', 'avg_mileage',
                                   'avg_engine_time', 'online_cnt'])

        if len(df) == 0:
            return df
        df['avg_oil_cost'] = df['avg_oil_cost'].astype(float)
        df['avg_mileage'] = df['avg_mileage'].astype(float)
        df['avg_engine_time'] = df['avg_engine_time'].astype(float) / 3600.0
        df['avg_oil_consumption_per_hour'] = df.apply(
            lambda row: row['avg_oil_cost'] / (row['avg_engine_time']) if row[
                                                                              'avg_engine_time'] != 0 else 0,
            axis=1)

        return df[['clct_date', 'car_model_id', 'model_name', 'avg_oil_cost', 'avg_mileage', 'avg_engine_time',
                   'avg_oil_consumption_per_hour', 'online_cnt']]

    def get_electric_consumption(self, date):
        query = self.common_sql + """
                
        SELECT 
            clct_date_ts::date AS clct_date,
            car_model_id,
            tcm.model_name,
            SUM(power_cost) / count(1) AS avg_power_cost,
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
            tcm.energy_type = 2 AND clct_date_ts = %s AND time_zone =8 and {}
        GROUP BY 
            clct_date_ts::date, car_model_id, tcm.model_name;
        """.format(self.cond)
        self.cursor.execute(query, (date,))
        df = pd.DataFrame(self.cursor.fetchall(),
                          columns=['clct_date', 'car_model_id', 'model_name', 'avg_power_cost', 'avg_mileage',
                                   'avg_engine_time', 'online_cnt'])
        if len(df) == 0:
            return df
        df['avg_power_cost'] = df['avg_power_cost'].astype(float)
        df['avg_mileage'] = df['avg_mileage'].astype(float)
        df['avg_engine_time'] = df['avg_engine_time'].astype(float) / 3600.0
        df['avg_power_cost_per_hour'] = df.apply(
            lambda row: row['avg_power_cost'] / (row['avg_engine_time']) if row[
                                                                                'avg_engine_time'] != 0 else 0,
            axis=1)
        return df[['clct_date', 'car_model_id', 'model_name', 'avg_power_cost', 'avg_mileage', 'avg_engine_time',
                   'avg_power_cost_per_hour', 'online_cnt']]

    def get_fuel_detail(self, date):
        query = self.common_sql + """
        SELECT 
            clct_date_ts::date AS clct_date,
            car_vin,
            terminal_id,
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
            tcm.energy_type = 1 AND clct_date_ts = %s AND time_zone =8 and {};
        """.format(self.cond)
        self.cursor.execute(query, (date,))
        df = pd.DataFrame(self.cursor.fetchall(),
                          columns=['clct_date', 'car_vin', 'terminal_id', 'car_model_id', 'model_name', 'oil_cost',
                                   'mileage', 'engine_time'])
        if len(df) == 0:
            return df
        df['oil_cost'] = df['oil_cost'].astype(float)
        df['mileage'] = df['mileage'].astype(float)
        df['engine_time'] = df['engine_time'].astype(float) / 3600.0
        df['oil_cost_per_100km'] = df.apply(
            lambda row: row['oil_cost'] / row['mileage'] * 100 if row['mileage'] != 0 else 0, axis=1)
        df['avg_oil_cost_per_hour'] = df.apply(
            lambda row: row['oil_cost'] / row['engine_time'] if row['engine_time'] != 0 else 0, axis=1)
        return df

    def get_recently_fuel(self, date):
        query = self.common_sql + """
        
        SELECT 
            clct_date_ts::date AS clct_date,
            SUM(oil_cost) AS total_oil_cost,
            SUM(mileage) AS total_mileage,
            count(1) as record
        FROM 
            t_o_vehicule_day td
        left JOIN 
            t_car tc ON td.dev_id = tc.terminal_id
        left JOIN 
            m tcm ON tc.car_model_id = tcm.model_id
        WHERE 
            tcm.energy_type = 1 AND clct_date_ts >= cast(%s as timestamp) - interval '6 days' AND clct_date_ts <= %s and time_zone = 8
        GROUP BY 
            clct_date_ts::date
        ORDER BY 
            clct_date_ts::date;
        """

        self.cursor.execute(query, (date, date,))
        df = pd.DataFrame(self.cursor.fetchall(),
                          columns=['clct_date', 'total_oil_cost', 'total_mileage','record'])
        msgs = []
        if len(df) == 0:
            msg = '{}日传统车统计缺失'.format( date)
            msgs.append(msg)
            return df,msgs
        df['total_oil_cost'] = df['total_oil_cost'].astype(float)
        df['total_mileage'] = df['total_mileage'].astype(float)

        # 计算每日的变化百分比
        df['mileage_change_pct'] = df['total_mileage'].pct_change() * 100
        df['oil_cost_change_pct'] = df['total_oil_cost'].pct_change() * 100
        df['record_change_pct'] = df['record'].pct_change() * 100




    # 检测异常

        mileage_change = df.loc[6, 'mileage_change_pct']
        oil_change = df.loc[6, 'oil_cost_change_pct']
        record_change = df.loc[6, 'record_change_pct']
        end_date = df.loc[len(df) - 1, 'clct_date'].strftime('%Y-%m-%d')


        if end_date != (date):
            msg = '{} 传统车统计缺失'.format( date)
            msgs.append(msg)
        else:


            if record_change < -5:
                value = Decimal(record_change)
                formatted_value = value.quantize(Decimal('0.00'), rounding=ROUND_DOWN)
                msg = "传统车条数异常,波动{:.2f}%\n{}\n{}" \
                    .format(formatted_value
                            , "{} 条数:{}".format(df.loc[6, 'clct_date'], df.loc[6, 'record'])
                            , "{} 条数:{}".format(df.loc[6 - 1, 'clct_date'], df.loc[6 - 1, 'record']))
                msgs.append(msg)


            if mileage_change < self.threshold_dec or mileage_change > self.threshold_inc:
                value = Decimal(mileage_change)
                formatted_value = value.quantize(Decimal('0.00'), rounding=ROUND_DOWN)
                msg = "传统车里程异常,波动 {:.2f}%\n{}\n{}" \
                    .format( formatted_value
                            , "{} 总里程:{}km".format(df.loc[6, 'clct_date'], df.loc[6, 'total_mileage'])
                            , "{} 总里程:{}km".format(df.loc[6 - 1, 'clct_date'], df.loc[6 - 1, 'total_mileage']))
                msgs.append(msg)


            if oil_change < self.threshold_dec or oil_change > self.threshold_inc:
                value = Decimal(record_change)
                formatted_value = value.quantize(Decimal('0.00'), rounding=ROUND_DOWN)
                msg = "传统车油耗异常,波动 {:.2f}%\n{}\n{}" \
                    .format(formatted_value
                            , "{} 总油耗:{}L".format(df.loc[6, 'clct_date'], df.loc[6, 'total_oil_cost'])
                            , "{} 总油耗:{}L".format(df.loc[6 - 1, 'clct_date'], df.loc[6 - 1, 'total_oil_cost']))
                msgs.append(msg)



        return df,msgs

    def get_recently_elec(self, date):
        query = self.common_sql + """
        
        SELECT 
            clct_date_ts::date AS clct_date,
            SUM(power_cost) AS total_power_cost,
            SUM(mileage) AS total_mileage,
            count(1) as record
        FROM 
            t_o_vehicule_day td
        JOIN 
            t_car tc ON td.dev_id = tc.terminal_id
        JOIN 
            m tcm ON tc.car_model_id = tcm.model_id
        WHERE 
            tcm.energy_type = 2 AND clct_date_ts >= cast(%s as timestamp) - interval '6 days' AND clct_date_ts <= %s and time_zone = 8
        GROUP BY 
            clct_date_ts::date
        ORDER BY 
            clct_date_ts::date;
        """
        self.cursor.execute(query, (date, date,))
        df = pd.DataFrame(self.cursor.fetchall(),
                          columns=['clct_date', 'total_power_cost', 'total_mileage'])
        msgs = []
        if len(df) == 0:
            msg = '{}日新能源统计缺失'.format( date)
            msgs.append(msg)
            return df, msgs
        df['total_power_cost'] = df['total_power_cost'].astype(float)
        df['total_mileage'] = df['total_mileage'].astype(float)

        # 计算每日的变化百分比
        df['mileage_change_pct'] = df['total_mileage'].pct_change() * 100
        df['power_cost_change_pct'] = df['total_power_cost'].pct_change() * 100
        df['record_change_pct'] = df['record'].pct_change() * 100

        mileage_change = df.loc[6, 'mileage_change_pct']
        power_change = df.loc[6, 'power_cost_change_pct']
        record_change = df.loc[6, 'record_change_pct']
        end_date = df.loc[len(df) - 1, 'clct_date'].strftime('%Y-%m-%d')


        if end_date != (date):
            msg = '{}日新能源统计缺失'.format( date)
            msgs.append(msg)

        value = Decimal(record_change)
        formatted_value = value.quantize(Decimal('0.00'), rounding=ROUND_DOWN)
        if record_change < -5:
            msg = "新能源条数异常,波动 {:.2f}%\n{}\n{}" \
                .format(formatted_value
                        , "{} 条数:{}".format(df.loc[6, 'clct_date'], df.loc[6, 'record'])
                        , "{} 条数:{}".format(df.loc[6 - 1, 'clct_date'], df.loc[6 - 1, 'record']))
            msgs.append(msg)

        value = Decimal(mileage_change)
        formatted_value = value.quantize(Decimal('0.00'), rounding=ROUND_DOWN)
        if mileage_change < self.threshold_dec or mileage_change > self.threshold_inc:
            msg = "新能源里程异常,波动 {:.2f}%\n{}\n{}" \
                .format(formatted_value
                        , "日期:{},总里程:{}km".format(df.loc[6, 'clct_date'], df.loc[6, 'total_mileage'])
                        , "日期:{},总里程:{}km".format(df.loc[6 - 1, 'clct_date'], df.loc[6 - 1, 'total_mileage']))
            msgs.append(msg)

        value = Decimal(power_change)
        formatted_value = value.quantize(Decimal('0.00'), rounding=ROUND_DOWN)
        if power_change < self.threshold_dec or power_change > self.threshold_inc:
            msg = "新能源电耗异常,波动 {:.2f}%\n{}\n{}" \
                .format( formatted_value
                        , "日期:{},总电耗:{}kw".format(df.loc[6, 'clct_date'], df.loc[6, 'total_power_cost'])
                        , "日期:{},总电耗:{}kw".format(df.loc[6 - 1, 'clct_date'], df.loc[6 - 1, 'total_power_cost']))
            msgs.append(msg)



        return df,msgs

    def get_total_consumption_electric(self, date):
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
            SUM(power_cost) AS total_power_cost,
            SUM(mileage) AS total_mileage
        FROM 
            t_o_vehicule_day td
        JOIN 
            t_car tc ON td.dev_id = tc.terminal_id
        JOIN 
            m tcm ON tc.car_model_id = tcm.model_id
        WHERE 
            tcm.energy_type = 2 AND clct_date_ts BETWEEN %s - interval '6 days' AND %s AND time_zone =8 and {}
        GROUP BY 
            clct_date_ts::date
        ORDER BY 
            clct_date_ts::date;
        """.format(self.cond)
        self.cursor.execute(query, (date, date,))
        df = pd.DataFrame(self.cursor.fetchall(),
                          columns=['clct_date', 'total_power_cost', 'total_mileage'])
        if len(df) == 0:
            return df
        df['total_power_cost'] = df['total_power_cost'].astype(float)
        df['total_mileage'] = df['total_mileage'].astype(float)
        return df

    def get_electric_detail(self, date):
        query = self.common_sql + """
        
        SELECT 
            clct_date_ts::date AS clct_date,
            car_vin,
            terminal_id,
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
            tcm.energy_type = 2 AND clct_date_ts::date = %s AND time_zone = 8 and {} ;
        """.format(self.cond)
        self.cursor.execute(query, (date,))
        df = pd.DataFrame(self.cursor.fetchall(),
                          columns=['clct_date', 'car_vin', 'terminal_id', 'car_model_id', 'model_name', 'power_cost',
                                   'mileage', 'engine_time'])
        if len(df) == 0:
            return df
        df['power_cost'] = df['power_cost'].astype(float)
        df['mileage'] = df['mileage'].astype(float)
        df['engine_time'] = df['engine_time'].astype(float) / 3600.0
        df['power_cost_per_100km'] = df.apply(
            lambda row: row['power_cost'] / row['mileage'] * 100 if row['mileage'] != 0 else 0, axis=1)
        return df

    def write_to_excel(self, dfs, filename):

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            fuel = u'传统车'
            elec = u'新能源'
            fuel_detail = u'传统车明细'
            elec_detail = u'新能源明细'
            recently_fuel = u'传统车近7天统计'
            recently_elec = u'新能源近7天统计'

            # 添加柱状图到 Fuel Avg Consumption
            if self.enable_fuel:
                dfs['fuel_avg'].to_excel(writer, sheet_name=fuel, index=False)
                dfs['recently_fuel'].to_excel(writer, sheet_name=recently_fuel, index=False)
                dfs['fuel_detail'].to_excel(writer, sheet_name=fuel_detail, index=False)
                ws_fuel_avg = writer.sheets[fuel]
                # 添加柱状图到 Total Consumption
                ws_total_consumption = writer.sheets[recently_fuel]
                self.add_bar_chart(u"总里程", dfs['recently_fuel'], ws_total_consumption, 1, 3, "K2")
                self.add_bar_chart(u"总油耗", dfs['recently_fuel'], ws_total_consumption, 1, 2, "K17")
                self.add_bar_chart(u"总条数", dfs['recently_fuel'], ws_total_consumption, 1, 4, "K31")

                self.add_bar_chart(u"平均油耗/小时", dfs['fuel_avg'], ws_fuel_avg, 3, 7, "K2")
                self.add_bar_chart(u"平均油耗", dfs['fuel_avg'], ws_fuel_avg, 3, 4, 'K17')
                self.add_bar_chart(u"平均里程", dfs['fuel_avg'], ws_fuel_avg, 3, 5, 'K31')
                self.add_bar_chart(u"平均时长", dfs['fuel_avg'], ws_fuel_avg, 3, 6, 'K45')

            if self.enable_elec:
                dfs['electric_avg'].to_excel(writer, sheet_name=elec, index=False)
                dfs['electric_detail'].to_excel(writer, sheet_name=elec_detail, index=False)
                dfs['recently_elec'].to_excel(writer, sheet_name=recently_elec, index=False)
                ws_recently_elec = writer.sheets[recently_elec]
                self.add_bar_chart(u"总里程", dfs['recently_elec'], ws_recently_elec, 1, 3, "K2")
                self.add_bar_chart(u"总电耗", dfs['recently_elec'], ws_recently_elec, 1, 2, "K17")
                self.add_bar_chart(u"总条数", dfs['recently_elec'], ws_recently_elec, 1, 4, "K31")
                # 添加柱状图到 Electric Avg Consumption
                ws_electric_avg = writer.sheets[elec]

                self.add_bar_chart(u"平均电耗/小时", dfs['electric_avg'], ws_electric_avg, 3, 7, "K2")
                self.add_bar_chart(u"平均电耗", dfs['electric_avg'], ws_electric_avg, 3, 4, 'K16')
                self.add_bar_chart(u"平均里程", dfs['electric_avg'], ws_electric_avg, 3, 5, 'K31')
                self.add_bar_chart(u"平均时长", dfs['electric_avg'], ws_electric_avg, 3, 6, 'K45')

        print(u"Excel report generated: {}".format(filename))

    def add_bar_chart(self, title, dfs, sheet, x, c1, column):
        chart = BarChart()
        chart.title = title
        chart.height = 6
        datas = Reference(sheet, min_col=c1, min_row=1,
                          max_row=len(dfs) + 1)
        labels = Reference(sheet, min_col=x, min_row=2,
                           max_row=len(dfs) + 1)
        chart.add_data(datas, titles_from_data=True)
        chart.set_categories(labels)

        # chart_electric_engine_time.dataLabels = True
        sheet.add_chart(chart, column)

    def generate_reports(self, date, dir):

        fuel_avg_df = None
        electric_avg_df = None
        fuel_detail_df = None
        electric_detail_df = None
        recently_fuel, fuel_msgs = None,[]
        recently_elec, elec_msgs= None,[]

        if self.config.get_quality_fuel_enable():
            fuel_avg_df = self.get_fuel_consumption(date)
            fuel_detail_df = self.get_fuel_detail(date)
            recently_fuel, fuel_msgs = self.get_recently_fuel(date)
        if self.config.get_quality_elec_enable():
            electric_avg_df = self.get_electric_consumption(date)
            electric_detail_df = self.get_electric_detail(date)
            recently_elec, elec_msgs= self.get_recently_elec(date)

        msgs = []
        idx = 1
        if len(fuel_msgs) > 0:
            for m in fuel_msgs:
                msgs.append('{}. {}'.format(idx,m))
                idx += 1
        if len(elec_msgs) > 0:
            for m in elec_msgs:
                msgs.append('{}. {}'.format(idx,m))
                idx += 1
        if len(msgs) > 0:
            self.wx.send(alarm(self.config.project, self.service, date,
                               '日统计异常', '\n' + '\n'.join(msgs)))

        dfs = {
            'fuel_avg': fuel_avg_df,
            'electric_avg': electric_avg_df,
            'fuel_detail': fuel_detail_df,
            'electric_detail': electric_detail_df,
            'recently_fuel': recently_fuel,
            'recently_elec': recently_elec,
        }
        path = u'{}/{}日统计{}.xlsx'.format(dir, self.config.get_project().decode('utf-8'), date)
        self.write_to_excel(dfs, path)

    def process(self, target_date, dir):
        self.generate_reports(target_date, dir)


def alarm(project, service, date, content, detail):
    markdown = """
<font color = warning >{project}告警</font>
><font color = info >服务:</font>  {service}
><font color = info >检测时间:</font>  {timestamp}
><font color = info >统计日期:</font>  {date}
><font color = info >报警内容:</font>  {content}
><font color = info >报警明细:</font> {detail}
"""

    return markdown.format(
        project=project,
        service=service,
        content=content,
        detail=detail,
        date=date,
        timestamp=cm.get_time()
    )
