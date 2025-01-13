# coding:utf-8
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory

# 创建一个带有文件历史记录的会话
session = PromptSession(history=FileHistory('.kafka_tool_history'))

while True:
    try:
        user_input = session.prompt('>>> ')
        if user_input.strip().lower() == 'exit':
            print("退出程序...")
            break
        elif user_input.strip():
            # 执行用户输入的命令...
            print("执行命令: {}".format(user_input))
            # 注意：实际应用中应替换为具体逻辑

    except (EOFError, KeyboardInterrupt):
        print("\n检测到 Ctrl+D 或 Ctrl+C，正在退出程序...")
        break