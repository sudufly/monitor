# coding:utf-8
from component.spring_monitor import SpringMonitor


class MenuItem:
    def __init__(self, name, action=None):
        self.name = name
        self.action = action
        self.submenu = None

    def add_submenu(self, menu):
        self.submenu = menu

    def execute(self):
        if self.action:
            self.action()
        elif self.submenu:
            self.submenu.show()


class Menu:
    def __init__(self, title):
        self.title = title
        self.items = []

    def add_item(self, item):
        self.items.append(item)

    def show(self):
        while True:
            print("\n{}".format(self.title))
            for i, item in enumerate(self.items, start=1):
                print("{}. {}".format(i, item))
            print("0. 返回上一级")

            choice = raw_input("请输入您的选择: ")
            if choice == '0':
                break
            try:
                selected_index = int(choice) - 1
                if 0 <= selected_index < len(self.items):
                    self.items[selected_index].execute()
                else:
                    print("无效的选择，请重新输入.")
            except ValueError:
                print("无效的选择，请重新输入.")


def module1_function1():
    print("执行 模块1 功能1")


def module1_function2_subfunction1():
    print("执行 模块1 功能2 子功能1")


def module1_function2_subfunction2():
    print("执行 模块1 功能2 子功能2")


def module2_function1_subfunction2():
    print("执行 模块2 功能1 子功能2")


# 创建菜单结构
module1_submenu = Menu("模块1 功能2")
module1_submenu.add_item(MenuItem("子功能1", module1_function2_subfunction1))
module1_submenu.add_item(MenuItem("子功能2", module1_function2_subfunction2))

module1_menu = Menu("模块1")
module1_menu.add_item(MenuItem("功能1", module1_function1))
module1_menu.add_item(MenuItem("功能2"))
module1_menu.items[-1].add_submenu(module1_submenu)

module2_submenu = Menu("模块2 功能1")
module2_submenu.add_item(MenuItem("子功能2", module2_function1_subfunction2))

module2_menu = Menu("模块2")
module2_menu.add_item(MenuItem("功能1"))
module2_menu.items[-1].add_submenu(module2_submenu)

main_menu = Menu("主菜单")
main_menu.add_item(MenuItem("模块1"))
main_menu.items[-1].add_submenu(module1_menu)
main_menu.add_item(MenuItem("模块2"))
main_menu.items[-1].add_submenu(module2_menu)

if __name__ == "__main__":
    spring = SpringMonitor()

    spring.list()
