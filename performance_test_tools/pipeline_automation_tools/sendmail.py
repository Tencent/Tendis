#!/usr/bin/python
# -*- coding: UTF-8 -*-

import sys
from pytof4 import Tof4Mail

PAASID = sys.argv[3]
TOKEN = sys.argv[4]
URL = sys.argv[5]
msg = Tof4Mail(PAASID, TOKEN, URL)
msg.Title = sys.argv[1] + "性能测试报告"
with open(sys.argv[2],'r') as f:
    s=''
    for l in f.readlines():
        s+='<p>'+l.replace('\n', '')+'</p>'
    msg.Content=s
msg.From = sys.argv[6]
if len(sys.argv) > 7:
    msg.To = sys.argv[7]
else:
    msg.To = ''
msg.CC = ''
msg.Bcc = ''
msg.attachment = []
msg.send()
