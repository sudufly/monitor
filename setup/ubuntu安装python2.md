Ubuntu 安装 Python2
Ubuntu 20.04+ 默认仓库移除 Python2，只能手动编译；Ubuntu 18.04 还可以 apt 安装。
⚠️不要修改系统默认 python3 软链接，部分系统工具依赖 python3
# 方式 1：Ubuntu 18.04（直接 apt）
bash
sudo apt update
sudo apt install -y python2 python2.7
验证：
bash
python2 --version
# 方式 2：Ubuntu 20.04 / 22.04 / 24.04（编译安装 Python‑2.7.18，Python2 最终版本）
## 1. 安装编译依赖
bash
sudo apt update
sudo apt install -y build-essential libffi-dev libssl-dev zlib1g-dev libbz2-dev libncurses5-dev libsqlite3-dev libreadline-dev tk-dev
## 2. 下载源码
bash
cd /usr/local/src
sudo wget https://www.python.org/ftp/python/2.7.18/Python-2.7.18.tgz
sudo tar -zxvf Python-2.7.18.tgz
cd Python-2.7.18
## 3. 编译，使用 altinstall，禁止 make install，防止覆盖系统 python
bash
sudo ./configure --prefix=/usr/local/python27 --enable-shared
sudo make
sudo make altinstall
## 4. 配置动态链接库
bash
echo "/usr/local/python27/lib" | sudo tee /etc/ld.so.conf.d/python27.conf
sudo ldconfig
## 5. 建立软链接
bash
sudo ln -s /usr/local/python27/bin/python2.7 /usr/bin/python2
sudo ln -s /usr/local/python27/bin/pip2.7 /usr/bin/pip2
## 6. 验证
bash
python2 --version
pip2 --version
输出 Python 2.7.18 成功。

## 7. 安装pip2
curl https://bootstrap.pypa.io/pip/2.7/get-pip.py -o get-pip.py
python2 get-pip.py
