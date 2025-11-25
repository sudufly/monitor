# coding:utf-8

import requests

from common import common as cm


class WxClient(object):
    webHookUrl = ''
    heartWebHookUrl = ''
    project = ''

    def __init__(self):

        self.initConfig()

    def send(self, text,heart = False):
        if heart:
            self.send_msg(text, self.heartWebHookUrl)
        else:
            self.send_msg(text, self.webHookUrl)


    def sendMarkDown(self, service, text):
        self.send_msg(generate_markdown(self.project, service, text), self.webHookUrl)

    def send_msg(self, textContent, webHookUrl, mentioned_list=[], mentioned_mobile_list=[]):
        """
        发送微信群组机器人消息
        :param textContent: 消息内容
        :param webHookUrl: 群组机器人WebHook
        :param mentioned_list: userid的列表，提醒群中的指定成员(@某个成员)，@all表示提醒所有人
        :param mentioned_mobile_list: 手机号列表，提醒手机号对应的群成员(@某个成员)，@all表示提醒所有人
        :return:
        """
        # url为群组机器人WebHook，配置项
        url = webHookUrl
        headers = {
            "content-type": "application/json"
        }
        msgtype = 'markdown'
        msg = {"msgtype": "markdown",
               "markdown": {
                   "content": textContent,
                   "mentioned_list": mentioned_list,
                   "mentioned_mobile_list": mentioned_mobile_list  # 替换为实际的手机号码
               }

               }  # 发送文本消息27     # 发送请求

        # msg = {"msgtype": "text",
        #        "text": {
        #            "content": textContent,
        #            "mentioned_list": mentioned_list,
        #            "mentioned_mobile_list": mentioned_mobile_list  # 替换为实际的手机号码
        #        }
        #
        #        }  # 发送文本消息27     # 发送请求
        try:
            result = requests.post(url, headers=headers, json=msg,verify = False)
            return True
        except Exception as e:
            print("[wxCon] send Requset Failed:", e)
            return False

    def initConfig(self):
        config = cm.getConfig()
        self.webHookUrl = config['weCom']['webHookUrl']
        self.heartWebHookUrl = config['weCom']['heartWebHookUrl']
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
