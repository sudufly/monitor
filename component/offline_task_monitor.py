# coding:utf-8
"""
行程离线统计任务监控

每天凌晨 6 点(与质量日报一起)检测前一日的 NORMAL 批次行程离线统计任务是否完成，
未完成或缺失时推送告警。由 config.ini 的 [quality] batchRouteEnable 开关控制。

数据表：hqweb.t_drive_route_task(行程离线任务日志表)
    task_date          统计日期(业务日)
    batch_mode         批次模式，只监控 NORMAL
    state              0-创建 1-未完成 2-完成
    task_info          任务信息, {"taskList": [{"date": "2026-09-15", "size": 43}], "startTime": "..."}
    task_process_info  任务执行信息, {"taskList": [{"date": "2026-09-15", "cost": 38, "size": 43}]}
"""
import json
import sys
from datetime import datetime

import psycopg2

from common import common as cm
from component.wx_client import WxClient
from config.config import Config

service = '行程离线任务检测'

# 监控的批次模式
BATCH_MODE = 'NORMAL'

# state 取值
STATE_CREATED = 0
STATE_UNFINISHED = 1
STATE_FINISHED = 2


def to_text(value):
    """统一转成文本，兼容 py2 的 str/unicode 和 py3 的 bytes/str"""
    if value is None:
        return ''
    if isinstance(value, (bytes, bytearray)):
        return value.decode('utf-8', 'ignore')
    return u'{}'.format(value)


def validate_task_date(value):
    text = to_text(value)
    for date_format in ('%Y%m%d', '%Y-%m-%d'):
        try:
            parsed = datetime.strptime(text, date_format)
            if parsed.strftime(date_format) == text:
                return parsed.strftime('%Y-%m-%d')
        except ValueError:
            pass
    raise ValueError('Invalid task date {!r}; expected YYYYMMDD (e.g. 20260101) or YYYY-MM-DD'.format(text))


def alarm(project, service, task_date, content, detail):
    markdown = u"""
<font color = warning >{project}告警</font>
><font color = info >服务:</font>  {service}
><font color = info >检测时间:</font>  {timestamp}
><font color = info >统计日期:</font>  {date}
><font color = info >报警内容:</font>  {content}
><font color = info >报警明细:</font> {detail}
"""

    return markdown.format(
        project=to_text(project),
        service=to_text(service),
        content=to_text(content),
        detail=to_text(detail),
        date=to_text(task_date),
        timestamp=cm.get_time()
    )


def err(project, service, content):
    markdown = u"""
<font color = warning >{project}告警 [异常]</font>
><font color = info >服务:</font>  {service}
><font color = info >检测时间:</font>  {timestamp}
><font color = info >异常:</font>  {content}
"""

    return markdown.format(
        project=to_text(project),
        service=to_text(service),
        content=to_text(content),
        timestamp=cm.get_time()
    )


class OfflineTaskMonitor(object):
    config = Config()
    wx = WxClient()

    conn = None

    def parse_jdbc_url(self, jdbc_url):
        """解析 config.ini 中的 jdbc url"""
        jdbc_url = to_text(jdbc_url)
        prefix = 'jdbc:postgresql://'
        if jdbc_url.startswith(prefix):
            jdbc_url = jdbc_url[len(prefix):]
        host_port, _, database = jdbc_url.partition('/')
        host, _, port = host_port.partition(':')
        return {
            'host': host,
            'port': int(port) if len(port) > 0 else 5432,
            'database': database
        }

    def get_connection(self):
        """获取数据库连接，断线自动重连"""
        if self.conn is not None and self.conn.closed == 0:
            return self.conn

        parsed = self.parse_jdbc_url(self.config.get_db_url())
        conn = psycopg2.connect(
            host=parsed['host'],
            user=self.config.get_db_user(),
            password=self.config.get_db_password(),
            dbname=parsed['database'],
            port=parsed['port'],
            connect_timeout=10
        )
        # 只读 + 自动提交：监控只查数据，不产生长事务
        conn.set_session(readonly=True, autocommit=True)
        self.conn = conn
        return conn

    def close(self):
        if self.conn is not None and self.conn.closed == 0:
            self.conn.close()
        self.conn = None

    def query_task_stats(self, task_date):
        """按批次模式、状态统计指定统计日期的任务，返回 {batch_mode: info}"""
        sql = """
            SELECT batch_mode, state, task_info, create_time, update_time
            FROM hqweb.t_drive_route_task
            WHERE task_date = %s
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (task_date,))
            rows = cursor.fetchall()
        finally:
            cursor.close()

        stats = {}
        for batch_mode, state, task_info, create_time, update_time in rows:
            batch_mode = to_text(batch_mode)
            info = stats.get(batch_mode)
            if info is None:
                info = {
                    'batch_mode': batch_mode,
                    'created': 0,
                    'unfinished': 0,
                    'finished': 0,
                    'other_state': 0,
                    'trip_size': 0,
                    'last_update': None
                }
                stats[batch_mode] = info

            if state == STATE_FINISHED:
                info['finished'] += 1
                info['trip_size'] += self.get_trip_size(task_info, task_date)
            elif state == STATE_UNFINISHED:
                info['unfinished'] += 1
            elif state == STATE_CREATED:
                info['created'] += 1
            else:
                info['other_state'] += 1

            info['last_update'] = self.max_time(info['last_update'], update_time)

        return stats

    def get_trip_size(self, task_info, task_date):
        """从 task_info 中取统计日期对应的行程条数"""
        info = self.to_dict(task_info)
        task_list = info.get('taskList') or info.get('task_list') or []
        if not isinstance(task_list, list):
            return 0

        size = 0
        for task in task_list:
            if not isinstance(task, dict):
                continue
            if to_text(task.get('date'))[:10] != to_text(task_date):
                continue
            size += self.to_int(task.get('size', task.get('cnt', 0)))
        return size

    def to_dict(self, value):
        """jsonb 字段转 dict，psycopg2 一般已自动转好，这里兼容字符串的情况"""
        if isinstance(value, dict):
            return value
        text = to_text(value).strip()
        if len(text) == 0:
            return {}
        try:
            data = json.loads(text)
        except ValueError:
            return {}
        return data if isinstance(data, dict) else {}

    def to_int(self, value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def max_time(self, current, value):
        if value is None:
            return current
        if current is None or value > current:
            return value
        return current

    def format_time(self, value):
        if isinstance(value, datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        return to_text(value)

    def build_detail(self, stats):
        """拼装检测明细"""
        lines = []
        info = stats.get(BATCH_MODE)
        if info is None:
            lines.append('{} 无任务记录'.format(BATCH_MODE))
        else:
            line = '{} 完成:{} 未完成:{} 创建:{}'.format(
                BATCH_MODE, info['finished'], info['unfinished'], info['created'])
            if info['other_state'] > 0:
                line = '{}, 其他状态:{}'.format(line, info['other_state'])
            if info['finished'] > 0:
                line = '{}, 昨日行程:{}条'.format(line, info['trip_size'])
            if info['last_update'] is not None:
                line = '{}, 最后更新:{}'.format(line, self.format_time(info['last_update']))
            lines.append(line)

        other_modes = [m for m in stats.keys() if m != BATCH_MODE]
        if len(other_modes) > 0:
            lines.append('其他批次: {}'.format(','.join(sorted(other_modes))))

        return '\n'.join(lines)

    def check(self, task_date=None):
        """检测指定统计日期(默认前一日)的 NORMAL 批次任务是否完成，返回 True 表示正常"""
        target_date = validate_task_date(task_date if task_date is not None else cm.get_yesterday_date())
        stats = self.query_task_stats(target_date)
        info = stats.get(BATCH_MODE)
        detail = self.build_detail(stats)

        if info is None or info['finished'] == 0:
            print('[offlineTask] {} {} 任务未完成\n{}'.format(target_date, BATCH_MODE, detail))
            self.wx.send(alarm(self.config.get_project(), service, target_date,
                               '{} 批次任务未完成'.format(BATCH_MODE), '\n' + detail))
            return False

        print('[offlineTask] {} {} 任务正常\n{}'.format(target_date, BATCH_MODE, detail))
        return True


if __name__ == '__main__':
    # 手动检测：python -m component.offline_task_monitor [YYYYMMDD 或 YYYY-MM-DD]
    monitor = OfflineTaskMonitor()
    date_arg = sys.argv[1] if len(sys.argv) >= 2 else None
    try:
        if date_arg is not None:
            validate_task_date(date_arg)
    except ValueError as exc:
        sys.stderr.write(str(exc) + '\n')
        sys.exit(2)
    try:
        monitor.check(task_date=date_arg)
    finally:
        monitor.close()
