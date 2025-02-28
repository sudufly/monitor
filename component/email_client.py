# -*- coding: utf-8 -*-
import os
import smtplib
from email import encoders
from email.MIMEText import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate

from common import common as cm
from config.config import Config


class EmailClient:
    config = Config()

    def send_zip_via_email(self, zip_path):
        project = self.config.get_project()
        smtp_server = 'smtp.exmail.qq.com'
        smtp_port = 465
        username = 'hqerp@hopechart.com'
        password = 'aRC5fjvUHrJ22nQT'
        from_addr = 'hqerp@hopechart.com'
        to_addrs = 'bo.xu@hopechart.com'

        msg = MIMEMultipart()
        msg['From'] = from_addr
        msg['To'] = to_addrs
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = u'{}-质量报告-{}'.format(project, cm.get_yesterday_date())

        body = ''
        msg.attach(MIMEText(body, 'plain', 'utf-8'))

        with open(zip_path, "rb") as file:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="{}"'.format(os.path.basename(zip_path)))
            msg.attach(part)

        try:
            server = smtplib.SMTP_SSL(smtp_server, smtp_port)
            server.login(username, password)
            server.sendmail(from_addr, to_addrs, msg.as_string())
            server.quit()
        except Exception as e:
            print("[Email] Failed to send email:", e)
