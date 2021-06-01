#!/usr/bin/python
# -*- coding: UTF-8 -*-

import sys
from pytof4 import Tof4Mail

PAASID = sys.argv[2]
TOKEN = sys.argv[3]
URL = sys.argv[4]
msg = Tof4Mail(PAASID, TOKEN, URL)
msg.Title = "Tendis-"+sys.argv[1]+"压力测试结果报告"
with open("Report.txt",'r') as f:
    s=''
    for l in f.readlines():
        s+='<p>'+l.replace('\n', '')+'</p>'
    msg.Content=s
msg.From = sys.argv[5]
if len(sys.argv) > 6:
    msg.To = sys.argv[6]
else:
    msg.To = ''
msg.CC = ''
msg.Bcc = ''
msg.attachment = []
msg.send()
