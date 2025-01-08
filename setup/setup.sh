#!/bin/bash
cd `dirname $0`
cd ..
path=$PWD
echo ---------$path----------------
base=$path/setup/monitor
target=$path/setup/monitor.service
workDir=$path/tool/
cp $base $target
appPath="/Type=simple/a\ExecStart=$workDir""monitor.py"
sed -i "$appPath" $target
appPath="/Type=simple/a\WorkingDirectory=$workDir"
sed -i "$appPath" $target

cp $target /usr/lib/systemd/system

rm -rf $target
chmod +x $workDir/*.py

systemctl daemon-reload


systemctl restart monitor.service
