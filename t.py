# coding:utf-8
from common import common as cm
from component.yarn_tool import YarnTool


class Context:
    data = [None] * 10
    pre_cmd = [None] * 10
    func = [None] * 10


class Menu:
    level = []
    context = Context()
    yarn = YarnTool()
    cur_func = None
    pre_func = None
    init_funcs = None
    content = '请输入序号'
    keys = set()
    level = 0

    def show_menu(self, menu_title, menu_items):
        print('*' * 80)
        print(menu_title.center(80))
        print('*' * 80)
        for item in menu_items:
            print(item)

    def handle_input(self, prompt, options, back_action=None):
        while True:
            try:

                choice = raw_input(prompt).strip().lower()
                print '{}-{}'.format(choice, options)
                if choice == 'exit':
                    exit(0)  # Exit the program
                elif choice == 'b':
                    self.level = self.level - 2 if self.level > 0 else 0

                    print 'back level:{} ,func:{}'.format(self.level, self.context.func[self.level])
                    self.context.func[self.level]()
                    # back_action()  # Go back to the previous menu
                    break
                elif choice in options:
                    return choice
                else:
                    print("无效输入,请重试")
            except KeyboardInterrupt:
                print("\n检测到 Ctrl+C，正在退出程序...")
                exit(0)

    def main_menu(self):
        self.show_menu('Main Menu', [
            "输入 'exit' 退出程序",
            "按[B]返回上一级 (仅适用于子菜单)",
            "1. 日志查询",
            "2. flink内存查询",
            "3. Flink Checkpoint",
        ])
        # self.cur_func = self.application_menu
        self.init_funcs = {'1': self.app_log_menu,
                           '2': self.mem_menu,
                           '3': self.view_menu
                           }
        self.keys = self.init_funcs.keys()
        self.context.func[0] = self.func_menu

        def inner_main_menu():
            while True:
                print("\n" + "-" * 120)

                choice = self.handle_input('>> {}: '.format(self.content), self.keys)
                self.menu_loop(choice)

        inner_main_menu()

    def menu_loop(self, choice=None):
        if self.cur_func is None:
            self.cur_func = self.init_funcs.get(choice)
            self.context.func[1] = self.cur_func
            self.level = 1
            print 'add:{}'.format(self.level)
        print 'pp--level:{} '.format(self.level)

        print '---------in level: {}-{}'.format(self.level, self.context.func)
        self.context.func[self.level]()
        print '---------out level{}-----------'.format(self.level)
        # self.cur_func(choice)

    def func_menu(self, choice=None):
        level = self.level
        pre_level = level - 1
        f = self.init_funcs[choice]
        self.pre_func[level] = f

    def app_log_menu(self, choice=None):
        apps = self.yarn.get_app_list()

        level = self.level
        pre_level = level - 1
        self.keys = set()
        [self.keys.add(str(v['id']))
         for v in apps]

        cm.print_dataset('Application List', apps)
        self.content = '输入applicationId'

        self.context.data[level] = apps
        self.context.func[level] = self.cur_func
        self.context.func[level + 1] = self.app_attempt_menu
        # self.pre_func = self.cur_func
        # self.cur_func = self.app_attempt_menu

        self.level += 1

    def app_attempt_menu(self, choice=None):
        level = self.level
        pre_level = level - 1
        applicationId = self.context.data[pre_level][int(choice) - 1]['applicationId']
        print applicationId
        attempts = self.yarn.get_app_attempts(applicationId)
        # self.context.data = attempts
        if len(attempts) > 0:
            self.keys = set()
            [self.keys.add(str(v['id']))
             for v in attempts]
            self.content = '输入attemptId'
            self.context.data[self.level] = attempts
            self.context.func[self.level] = self.cur_func
            self.context.func[self.level + 1] = self.app_attempt_detail_menu

            self.level += 1

    def app_attempt_detail_menu(self, choice=None):
        info = self.context.data[int(choice) - 1]
        attempt_id = info['attemptId']
        type = info['type']
        i = self.yarn.get_attempt_info(type, 'application_1736924516215_0001', attempt_id, '')
        self.context.func[self.level] = self.cur_func
        self.level += 1

        print i

    def mem_menu(self, choice=None):
        print 'flink mem'

    def view_menu(self, choice=None):
        print 'fink view'


if __name__ == "__main__":
    menu = Menu()
    menu.main_menu()
