# coding:utf-8

import json
import re
import time

import requests

from common import common as cm


try:
    text_type = unicode
except NameError:
    text_type = str


class WxClient(object):
    webHookUrl = ''
    heartWebHookUrl = ''
    weComEnable = True
    feiShuWebHookUrl = ''
    feiShuHeartWebHookUrl = ''
    feiShuEnable = True
    project = ''
    feiShuMinIntervalSeconds = 0.35
    feiShuRetryDelays = [1, 3, 6]
    _lastFeiShuSendAt = 0

    def __init__(self):
        self.initConfig()

    def send(self, text, heart=False):
        targets = self._get_targets(heart)
        if len(targets) == 0:
            print("[wxCon] no webhook configured")
            return False

        success = False
        for webhook_url in targets:
            if self.send_msg(text, webhook_url):
                success = True
        return success

    def sendMarkDown(self, service, text):
        return self.send(generate_markdown(self.project, service, text))

    def send_msg(self, textContent, webHookUrl, mentioned_list=None, mentioned_mobile_list=None):
        """
        发送群组机器人消息，自动兼容企业微信和飞书
        :param textContent: 消息内容
        :param webHookUrl: 群组机器人WebHook
        :param mentioned_list: 企业微信userid列表
        :param mentioned_mobile_list: 企业微信手机号列表
        :return:
        """
        if mentioned_list is None:
            mentioned_list = []
        if mentioned_mobile_list is None:
            mentioned_mobile_list = []

        url = (webHookUrl or '').strip()
        if len(url) == 0:
            return False

        if self._is_feishu_webhook(url):
            return self._send_feishu_msg(textContent, url)
        return self._send_wecom_msg(textContent, url, mentioned_list, mentioned_mobile_list)

    def _send_wecom_msg(self, textContent, webHookUrl, mentioned_list, mentioned_mobile_list):
        headers = {
            "content-type": "application/json"
        }
        msg = {
            "msgtype": "markdown",
            "markdown": {
                "content": textContent,
                "mentioned_list": mentioned_list,
                "mentioned_mobile_list": mentioned_mobile_list
            }
        }
        try:
            response = requests.post(webHookUrl, headers=headers, json=msg, verify=False, timeout=10)
            return self._is_success_response(response, 'weCom')
        except Exception as e:
            print("[wxCon] send weCom request failed:", e)
            return False

    def _send_feishu_msg(self, textContent, webHookUrl):
        headers = {
            "content-type": "application/json"
        }
        msg = self._build_feishu_card(textContent)
        try:
            self._wait_for_feishu_slot()
            response = requests.post(webHookUrl, headers=headers, json=msg, verify=False, timeout=10)
            if self._is_success_response(response, 'feiShu'):
                self._mark_feishu_send()
                return True

            response_data = self._parse_response_json(response)
            if self._is_feishu_frequency_limited(response_data):
                return self._retry_feishu_msg(webHookUrl, headers, msg)
            return False
        except Exception as e:
            print("[wxCon] send feiShu request failed:", e)
            return False

    def _get_targets(self, heart=False):
        targets = []
        if heart:
            self._append_target(targets, self.weComEnable, self.heartWebHookUrl, self.webHookUrl)
            self._append_target(targets, self.feiShuEnable, self.feiShuHeartWebHookUrl, self.feiShuWebHookUrl)
        else:
            self._append_target(targets, self.weComEnable, self.webHookUrl)
            self._append_target(targets, self.feiShuEnable, self.feiShuWebHookUrl)
        return targets

    def _append_target(self, targets, enabled, primary_url, fallback_url=''):
        if not enabled:
            return
        target_url = primary_url
        if not self._has_value(target_url):
            target_url = fallback_url
        if not self._has_value(target_url):
            return
        if target_url not in targets:
            targets.append(target_url)

    def _has_value(self, value):
        return value is not None and len(value.strip()) > 0

    def _is_feishu_webhook(self, webHookUrl):
        return 'open.feishu.cn' in webHookUrl or 'open.larksuite.com' in webHookUrl

    def _normalize_feishu_text(self, textContent):
        text = self._to_unicode(textContent)
        text = self._convert_wecom_font_to_feishu(text)
        text = re.sub(r'^\s*>\s*', '', text, flags=re.MULTILINE)
        text = re.sub(r'[ \t]+\n', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _build_feishu_card(self, textContent):
        normalized_text = self._normalize_feishu_text(textContent)
        lines = [line.strip() for line in normalized_text.split('\n') if len(line.strip()) > 0]
        if len(lines) == 0:
            lines = ['监控消息']

        markdown_lines = [lines[0]]
        if len(lines) > 1:
            markdown_lines.append('')
            markdown_lines.extend(lines[1:])

        card = {
            "config": {
                "wide_screen_mode": True
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": '\n'.join(markdown_lines)
                    }
                }
            ]
        }

        return {
            "msg_type": "interactive",
            "card": json.dumps(card, ensure_ascii=False)
        }

    def _convert_wecom_font_to_feishu(self, text):
        color_map = {
            'info': 'green',
            'warning': 'orange',
            'comment': 'grey',
            'normal': 'grey',
            'red': 'red',
            'green': 'green',
            'blue': 'blue',
            'orange': 'orange',
            'grey': 'grey'
        }

        def replace_font(match):
            color = (match.group(1) or '').strip().lower()
            mapped_color = color_map.get(color, color)
            return u"<font color='{color}'>".format(color=mapped_color)

        return re.sub(r"<font\s+color\s*=\s*['\"]?([^'\"> ]+)['\"]?\s*>", replace_font, text, flags=re.IGNORECASE)

    def _is_success_response(self, response, channel):
        if response.status_code < 200 or response.status_code >= 300:
            print('[wxCon] send {channel} http failed: {status} {body}'.format(
                channel=channel,
                status=response.status_code,
                body=response.text
            ))
            return False

        data = self._parse_response_json(response)
        if channel == 'weCom':
            code = data.get('errcode')
            if code not in (None, 0):
                print('[wxCon] send weCom failed:', response.text)
                return False
        if channel == 'feiShu':
            code = data.get('code')
            status_code = data.get('StatusCode')
            if code not in (None, 0) or status_code not in (None, 0):
                print('[wxCon] send feiShu failed:', response.text)
                return False
        return True

    def _parse_response_json(self, response):
        try:
            return response.json()
        except Exception:
            return {}

    def _wait_for_feishu_slot(self):
        now = time.time()
        wait_seconds = self.feiShuMinIntervalSeconds - (now - self._lastFeiShuSendAt)
        if wait_seconds > 0:
            time.sleep(wait_seconds)

    def _mark_feishu_send(self):
        self._lastFeiShuSendAt = time.time()

    def _is_feishu_frequency_limited(self, data):
        if not isinstance(data, dict):
            return False
        return data.get('code') == 11232 or data.get('StatusCode') == 11232

    def _retry_feishu_msg(self, webHookUrl, headers, msg):
        for delay_seconds in self.feiShuRetryDelays:
            print('[wxCon] feiShu frequency limited, retry after {}s'.format(delay_seconds))
            time.sleep(delay_seconds)
            try:
                self._wait_for_feishu_slot()
                response = requests.post(webHookUrl, headers=headers, json=msg, verify=False, timeout=10)
                if self._is_success_response(response, 'feiShu'):
                    self._mark_feishu_send()
                    return True
                response_data = self._parse_response_json(response)
                if not self._is_feishu_frequency_limited(response_data):
                    return False
            except Exception as e:
                print("[wxCon] retry feiShu request failed:", e)
        return False

    def _to_unicode(self, value):
        if isinstance(value, text_type):
            return value
        try:
            return value.decode('utf-8')
        except Exception:
            try:
                return text_type(value)
            except Exception:
                return ''

    def _find_section_name(self, config, expected_name):
        for section_name in config.sections():
            if section_name.lower() == expected_name.lower():
                return section_name
        return None

    def _get_option(self, config, section_name, option_name):
        if section_name is None:
            return ''
        if not config.has_option(section_name, option_name):
            return ''
        return config.get(section_name, option_name).strip()

    def _get_bool_option(self, config, section_name, option_name, default_value=True):
        if section_name is None:
            return default_value
        if not config.has_option(section_name, option_name):
            return default_value
        value = config.get(section_name, option_name).strip().lower()
        return value in ('1', 'true', 'yes', 'on')

    def initConfig(self):
        config = cm.getConfig()
        wecom_section = self._find_section_name(config, 'weCom')
        feishu_section = self._find_section_name(config, 'feiShu')

        self.webHookUrl = self._get_option(config, wecom_section, 'webHookUrl')
        self.heartWebHookUrl = self._get_option(config, wecom_section, 'heartWebHookUrl')
        self.weComEnable = self._get_bool_option(config, wecom_section, 'enable', True)
        self.feiShuWebHookUrl = self._get_option(config, feishu_section, 'webHookUrl')
        self.feiShuHeartWebHookUrl = self._get_option(config, feishu_section, 'heartWebHookUrl')
        self.feiShuEnable = self._get_bool_option(config, feishu_section, 'enable', True)
        self.project = config['project']['project'].encode("utf-8")


def generate_markdown(project, service, content):
    markdown = """
<font color = warning >{project}平台告警</font>
><font color = info >服务:</font>  {service} 
><font color = info >触发时间:</font>  {timestamp} 
><font color = info >报警信息:</font> {content}
"""
    return markdown.format(
        project=project,
        service=service,
        content=content,

        timestamp=cm.get_time()
    )


if __name__ == "__main__":
    wxclient = WxClient()
    project = wxclient.project
    webHookUrl = wxclient.webHookUrl
    wxclient.send_msg(generate_markdown(project, 'kafka消费组监控', 'xxxy异常'), webHookUrl,
                      mentioned_list=['@all'], mentioned_mobile_list=['18716650692'])
