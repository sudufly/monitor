# 安装
sudo yum install python-pip==20.3.4
pip install --upgrade pip

pip install --trusted-host pypi.python.org --upgrade pip==20.3.4


pip install requests==2.6.0
pip install configparser==4.0.2

## kafka
pip install -U kafka-python==2.0.2


pip install logging
## 时区
pip install pytz
## 显示宽度
pip install wcwidth

pip install urllib3==1.10.2



#
sed -i 's/\r$//' x.py




## python 2
https://blog.csdn.net/2401_86454507/article/details/142166670

curl https://bootstrap.pypa.io/pip/2.7/get-pip.py -o get-pip.py



## 快速安装
cp tools/config.ini ./;
wget  -O ./monitor.zip https://codeload.github.com/sudufly/monitor/zip/refs/heads/main;
unzip -q monitor.zip -d ./;
cp  monitor-main/setup ./ -R;
chmod +x setup/*.sh;
setup/install.sh;
setup/update.sh;
cp config.ini tool/;
setup/setup.sh;
rm -rf tools;rm -rf monitor*;rm -rf yarn/;
