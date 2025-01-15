#!/bin/bash

cd `dirname $0`
cd ..
path=$PWD
echo ---------$path----------------
target=setup/package
mkdir -p $target
rm -rf $target/*
wget  -O $target/monitor.zip https://codeload.github.com/sudufly/monitor/zip/refs/heads/main

unzip -q $target/monitor.zip -d $target

echo ''
echo ''
echo ''
echo '-----------unzip finish---------'
echo '-----------start sync-----------'

#rsync -r -v --exclude *.ini $target/monitor-main/ ./tool/
#rsync -r -v --exclude *.ini $target/monitor-main/ ./tool/

if [ ! -d "$path/tool" ]; then
    echo "to create..."
    rsync -r  $target/monitor-main/ ./tool/
else
    echo "to update..."
    rsync -r  --exclude *.ini $target/monitor-main/ ./tool/
fi

sed -i 's/\r$//' $path/tool/*.py
chmod +x $path/tool/*.py
