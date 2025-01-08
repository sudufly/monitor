#!/bin/bash
cp monitor.service /usr/lib/systemd/system

systemctl daemon-reload


systemctl restart monitor.service
