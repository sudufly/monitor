# coding:utf-8
class DynamicMenu:
    def __init__(self, name):
        self.name = name
        self.menu_structure = {}  # 存储菜单结构的字典
        self.current_path = []    # 当前路径栈，用于记录当前所处的菜单层级
        self.command_history = [] # 命令历史栈

    def add_menu_item(self, path, key, item_type, action=None, description=""):
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

        if self.current_path:
            print("[B] Back to '{}'".format('/'.join(self.current_path[:-1]) or self.name))

        for key, item in current_level.items():
            if len(item) >= 3:
                _, _, desc = item[:3]
            else:
                desc = "No Description"

            print("[{}] {}".format(key, desc))

        choice = raw_input("\nEnter your choice: ").strip().upper()

        if choice == 'B' and self.current_path:
            self.return_to_parent()
        elif choice in current_level:
            item_type, value, _ = current_level[choice][:3]
            if item_type == 'action':
                self.execute_action(choice, value)
            elif item_type == 'submenu':
            # 检查是否有加载逻辑，并以字符串形式提供方法名
                load_method_name = value.get('load')
                load_method_name()
                # if load_method_name and hasattr(self, load_method_name):
                #     getattr(self, load_method_name)()  # 调用加载方法
                self.enter_submenu(choice)
        else:
            print("Invalid choice. Please try again.")
            self.show_menu()

    def execute_action(self, choice, action):
        self.command_history.append(choice)
        action()
        raw_input("Press Enter to continue...")
        self.show_menu()

    def enter_submenu(self, choice):
        self.command_history.append(choice)
        self.current_path.append(choice)
        self.show_menu()

    def return_to_parent(self):
        if self.command_history:
            print("Returning from last command: [{}]".format(self.command_history.pop()))
        if self.current_path:
            self.current_path.pop()
            self.show_menu()

    def load_apps_for_logs(self):
        """
        模拟从接口获取应用列表并动态添加到日志子菜单。
        """
        apps = fetch_apps_from_api()

        logs_submenu_key = 'L'
        if logs_submenu_key in self.menu_structure:
            del self.menu_structure[logs_submenu_key]

        self.add_menu_item([], logs_submenu_key, 'submenu', None, "Logs")

        logs_submenu = self.menu_structure.setdefault(logs_submenu_key, {})
        app_list_key = 'A'
        self.add_menu_item([logs_submenu_key], app_list_key, 'submenu', None, "App List")
        app_list_submenu = logs_submenu.setdefault(app_list_key, {})[1]

        for idx, (app_id, app_info) in enumerate(apps.items(), start=1):
            app_key = "A{}".format(idx)
            app_description = "App ID: {}".format(app_id)
            self.add_menu_item([logs_submenu_key, app_list_key], app_key, 'submenu', None, app_description)
            app_submenu = app_list_submenu.setdefault(app_key, {})[1]

            for func_name in app_info.get('functions', []):
                func_key = func_name.replace(' ', '_').lower()
                self.add_menu_item([logs_submenu_key, app_list_key, app_key], func_key, 'action',
                                   lambda x=func_name, y=app_id:  (x, y),
                                   func_name)

        self.current_path.extend([logs_submenu_key, app_list_key])
        self.show_menu()
def fetch_apps_from_api():
    """
    模拟从API获取应用列表的函数。
    实际使用时应替换为真实的API调用。
    """
    # 模拟的数据
    return {
        'appId_1': {'functions': ['功能1', '功能2']},
        'appId_2': {'functions': ['功能A', '功能B']}
    }
def action_hello():
    print("Hello, World!")

def action_goodbye():
    print("Goodbye!")

def exit_program():
    print("Exiting program...")
    sys.exit(0)
if __name__ == "__main__":
    menu = DynamicMenu("Main Menu")

    # 动态添加主菜单项
    menu.add_menu_item([], 'H', 'action', action_hello, "Say Hello")
    menu.add_menu_item([], 'G', 'action', action_goodbye, "Say Goodbye")
    menu.add_menu_item([], 'X', 'action', exit_program, "Exit Program")

    # 设置日志子菜单的加载逻辑
    menu.menu_structure['L'] = ('submenu', {'load': menu.load_apps_for_logs}, "Logs")

    # 启动菜单
    menu.show_menu()











