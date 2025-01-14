# coding:utf-8
import requests

import json
from pprint import pprint

# 配置Flink集群的JobManager地址和端口
FLINK_JM_URL = "http://hbase2-102:18088/proxy/application_1735628121942_0023"  # 替换为实际的JobManager URL

def get_job_overview():
    """获取所有作业的概述"""
    url = "{}/jobs/overview".format(FLINK_JM_URL)
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch job overview, status code: {}".format(response.status_code))
        return None

def get_job_details(job_id):
    """根据job_id获取特定作业的详细信息"""
    url = "{}/jobs/{}".format(FLINK_JM_URL, job_id)
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch job details for job {}, status code: {}".format(job_id, response.status_code))
        return None

def get_checkpoint_data(job_id):
    """根据job_id获取特定作业的checkpoint数据"""
    url = "{}/jobs/{}/checkpoints".format(FLINK_JM_URL, job_id)
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch checkpoint data for job {}, status code: {}".format(job_id, response.status_code))
        return None

def main():
    overview = get_job_overview()
    if not overview or 'jobs' not in overview:
        print("No jobs found.")
        return

    for job in overview['jobs']:
        print job
        job_id = job['jid']
        job_status = job['state']

        print("\nJob ID: {}\nStatus: {}".format(job_id, job_status))

        details = get_job_details(job_id)
        if details:
            job_name = details['name']
            print("Name: {}".format(job_name))

            checkpoints = get_checkpoint_data(job_id)
            if checkpoints and 'latest' in checkpoints:
                latest_checkpoints = checkpoints['latest']
                if 'completed' in latest_checkpoints:
                    completed_checkpoint = latest_checkpoints['completed']
                    print("Latest Completed Checkpoint:")
                    pprint(completed_checkpoint)

if __name__ == "__main__":
    main()