# coding:utf-8
import sys

from common import common as cm
from component.yarn_tool import YarnTool


class DynamicMenu:
    apps = None;
    yarn = None;

    def __init__(self, name):
        self.name = name
        self.menu_structure = {}  # 存储菜单结构的字典
        self.current_path = []  # 当前路径栈，用于记录当前所处的菜单层级
        self.command_history = []  # 命令历史栈
        self.init()

    def init(self):
        self.yarn = YarnTool()
        self.apps = self.yarn.get_app_list('Apache Flink')
        # cm.print_dataset('Application List', self.apps)

    def add_menu_item(self, path, key, item_type, action=None, description=""):
        """
        动态添加菜单项或子菜单。
        :param path: 列表形式的路径，指示要添加到哪个层级
        :param key: 菜单项的键（用户选择时输入）
        :param item_type: 'action' 或 'submenu'
        :param action: 如果是动作，则为对应的函数；如果是子菜单，则为None
        :param description: 菜单项描述
        """
        current_level = self.menu_structure
        for p in path:
            if p not in current_level or not isinstance(current_level[p], dict):
                current_level[p] = {}
            current_level = current_level[p]

        if item_type == 'action':
            current_level[key] = ('action', action, description)
        elif item_type == 'submenu':
            current_level[key] = ('submenu', {}, description)

    def show_menu(self):
        print("\n" + "=" * 40)
        print("Menu: " + '/'.join(self.current_path) or self.name)
        print("=" * 40)

        current_level = self.menu_structure
        for p in self.current_path:
            if p in current_level and isinstance(current_level[p], dict):
                current_level = current_level[p]
            else:
                break
        # print "cup:".format(self.current_path)
        if self.current_path:
            print("[B] Back to '{}'".format('/'.join(self.current_path[:-1]) or self.name))
        print current_level
        for key, item in current_level.items():
            if len(item) == 3:
                item_type, value, desc = item  # 确保有三个值可以解包
            # else:
            #     item_type, value = item  # 如果只有两个值，忽略描述
            #     desc = "No Description"  # 提供默认描述

            print("[{}] {}".format(key, desc))

        if self.command_history and self.current_path:
            print("\nLast command in this layer: [{}]".format(self.command_history[-1]))

        choice = raw_input("\nEnter your choice: ").strip().upper()

        if choice == 'B' and self.current_path:
            self.return_to_parent()
        elif choice in current_level:
            item_type, value, desc = current_level[choice]
            if item_type == 'action':
                self.execute_action(choice, value)
            elif item_type == 'submenu':
                self.enter_submenu(choice)
        else:
            print("Invalid choice. Please try again.")
            self.show_menu()

    def execute_action(self, choice, action):
        self.command_history.append(choice)  # 记录命令
        action()
        self.show_menu()

    def enter_submenu(self, choice):
        self.command_history.append(choice)  # 记录进入子菜单的选择
        self.current_path.append(choice)
        self.show_menu()

    def return_to_parent(self):
        if self.command_history:
            print("Returning from last command: [{}]".format(self.command_history.pop()))  # 弹出并显示最后的选择
        if self.current_path:
            self.current_path.pop()
            self.show_menu()

    def process_logs(self):
        cm.print_dataset('Application List', self.apps)
        submenu1 = ['dd']
        self.add_menu_item(submenu1, 'A', 'action', action_goodbye, "Say Hello from Submenu 1")


def action_goodbye():
    print("Goodbye!")


def exit_program():
    print("Exiting program...")
    sys.exit(0)


if __name__ == "__main__":
    menu = DynamicMenu("Main Menu")

    # 动态添加主菜单项
    menu.add_menu_item([], '1', 'action', menu.process_logs, "Logs")
    menu.add_menu_item([], '2', 'action', action_goodbye, "Memory View")
    # menu.add_menu_item([], 'X', 'action', exit_program, "Exit Program")

    # 动态添加子菜单及其项
    submenu1 = ['dd']
    # menu.add_menu_item(submenu1, 'A', 'action', action_hello, "Say Hello from Submenu 1")
    menu.add_menu_item(submenu1, 'B', 'submem', action_goodbye, "Say Goodbye from Submenu 1")

    submenu2 = ['ee']
    # menu.add_menu_item(submenu2, 'C', 'action', action_hello, "Say Hello from Submenu 2")
    menu.add_menu_item(submenu2, 'D', 'action', action_goodbye, "Say Goodbye from Submenu 2")

    # 启动菜单
    menu.show_menu()
