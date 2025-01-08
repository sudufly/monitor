# coding:utf-8
import time
import os
import configparser

# import wcwidth
import warnings
warnings.filterwarnings('ignore', category=UnicodeWarning)
def getConfig(debug=False):
    global yarn_url

    # 获取脚本所在的工作目录
    working_dir = os.getcwd()

    # 构建配置文件路径
    config_path = os.path.join(working_dir, 'config.ini')
    config = configparser.ConfigParser()
    # print config_path
    config.read(config_path, encoding='utf-8')
    # yarn_url = config['yarn']['rm_url']
    # 获取默认部分的值

    # 获取特定部分的值

    # 遍历所有部分和键值对
    if debug:
        for section in config.sections():
            print("Section: {}".format(section))
            for key, value in config.items(section):
                print("{}={}".format(key, value.encode('utf-8')))
    return config

def print_dataset(title, data, column_padding=1):
    if not data or not isinstance(data, list) or len(data) == 0:
        print("空数据集")
        return

    # 获取所有列名（假设所有字典有相同的键）
    headers = data[0].keys()

    # 计算每列的最大宽度（包括标题和数据）
    max_widths = {header: max(display_width(header), max(display_width(str(row[header])) for row in data)) for header in headers}


    # 定义边框字符和内边距
    border_char = '-'
    side_padding = ' ' * column_padding

    # 打印上边框
    total_length = sum(max_widths.values()) + (len(headers) + 1) * (column_padding * 2 + 1) - 1
    print(border_char * total_length)
    print('|' + title.center(total_length - 3) + '|')
    print(border_char * total_length)

    # 辅助函数：生成一行文本
    def make_row(cells):
        return '| ' + ' | '.join([str(cell).center(max_widths[header]) for header, cell in zip(headers, cells)]) + ' |'

    # 打印标题行
    title_cells = [header for header in headers]
    print(make_row(title_cells))

    # 打印分隔线
    separator = '+-' + '-+-'.join([border_char * max_widths[header] for header in headers]) + '-+'
    print(separator)

    # 打印数据行
    for row in data:
        data_cells = [row[header] for header in headers]
        print(make_row(data_cells))

    # 打印下边框
    print(border_char * total_length)



def utc_ms_to_time(utc_ms , format_str='%Y-%m-%d %H:%M:%S'):
    """
    将毫秒级的 UTC 时间戳转换为本地时间的格式化字符串。

    :param utc_ms: 毫秒级 UTC 时间戳
    :param format_str: 格式化字符串，默认为 '%Y-%m-%d %H:%M:%S'
    :return: 格式化后的本地时间字符串
    """
    # 将毫秒时间戳转换为秒，并去掉小数部分（如果有）
    seconds = utc_ms / 1000.0

    # 使用 localtime 将秒级时间戳转换为本地时间元组
    local_time_tuple = time.localtime(seconds)

    # 使用 strftime 格式化时间元组为字符串
    formatted_time = time.strftime(format_str, local_time_tuple)

    return formatted_time

def get_time():

    return utc_ms_to_time(time.time() * 1000)


def display_width(text):
    return len(text)
    # return wcwidth.wcswidth(text.decode('utf-8'))

# def display_width(text):
#     """计算文本的显示宽度"""
#     return sum(wcwidth.wcwidth(ord(c)) for c in text)
# text = "Hello, 世界"
#
#
# print(display_width(text))  # 输出: 17 (每个中文字符占据两个单元格)
#
#
#
#
# text = '世界'
# k = wcwidth.wcswidth(text.decode('utf-8'))
# print k