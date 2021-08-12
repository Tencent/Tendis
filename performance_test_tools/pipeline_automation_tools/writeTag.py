#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import re
import sqlite3
import sys


RecordDBPath='tendisTestResult.db'
if not os.path.exists(RecordDBPath):
    conn = sqlite3.connect(RecordDBPath)
    # tid only for index
    conn.execute('''
    CREATE TABLE result
    (
        tid INTEGER PRIMARY KEY,
        testname TEXT,
        version TEXT,
        date TEXT,
        qps REAL,
        p50 REAL,
        p99 REAL,
        p100 REAL,
        avg REAL
    )''')
    conn.commit()
    conn.close()

def getMaxTid():
    conn = sqlite3.connect(RecordDBPath)
    c=conn.cursor()
    p=c.execute('''
    select count(*) from result''')
    if not (p.fetchone())[0]:
        return -1
    p=c.execute('''
    select max(tid) from result''')
    res=p.fetchone()
    conn.commit()
    conn.close()
    return res[0]

def saveTestResult(testName, version, date, qps, p50, p99, p100, pavg):
    curMaxTid=getMaxTid()
    conn = sqlite3.connect(RecordDBPath)
    conn.execute('''
    insert into result values (?, ?, ?, ?, ?, ?, ?, ?, ?)''', (str(int(curMaxTid)+1), str(testName), str(version), str(date), str(qps), str(p50), str(p99), str(p100), str(pavg)))
    conn.commit()
    conn.close()

def getHistoryRecord(testName, version):
    conn = sqlite3.connect(RecordDBPath)
    c=conn.cursor()
    p=c.execute('''
    select count(*) from result where testname = ? and version = ?''', (str(testName),str(version)))
    if not (p.fetchone())[0]:
        return None
    p=c.execute('''
    select * from result where testname = ? and version = ?''', (str(testName), str(version)))
    res=p.fetchone()
    conn.commit()
    conn.close()
    return res

def prettyFormat(floatNum):
    # show 1.1w when floatNum is 11000 for human readable.
    if float(floatNum) > 10000.0:
        return "%.1fw" % (float(floatNum)/10000.0)
    else:
        return "%.1f" % (float(floatNum))

if __name__ == '__main__':
    if len(sys.argv) != 18:
        os._exit(0)

    testName=sys.argv[1]
    version=sys.argv[2]
    date=sys.argv[3]
    qps=sys.argv[4]
    p50=sys.argv[5]
    p99=sys.argv[6]
    p100=sys.argv[7]
    pavg=sys.argv[8]
    outputFile=sys.argv[9]
    decreaseLimit=sys.argv[10]
    decreaseLimitP50=sys.argv[11]
    decreaseLimitP99=sys.argv[12]
    decreaseLimitP100=sys.argv[13]
    decreaseLimitPavg=sys.argv[14]
    # 1 present 'save result to db'
    # other for 'not save'
    shouldSave=sys.argv[15] == "1"
    # 1 present 'compare to history record'
    # other for 'not compare'
    compareToHistory=sys.argv[16] == "1"
    baselineVersion=sys.argv[17]
    versionInfo=re.search(r'\d+', baselineVersion).group()

    if shouldSave:
        saveTestResult(testName, version, date, qps, p50, p99, p100, pavg)
    r=getHistoryRecord(testName, baselineVersion)
    f=open(outputFile,'a')
    f.write("<table style=\"border:1px solid black; collapse:collapse\">")
    f.write("<tr>")
    f.write("<th style=\"border:1px solid black; text-align:center collapse:collapse\"></th>")
    f.write("<th style=\"border:1px solid black; text-align:center collapse:collapse\">本次测试结果</th>")
    if compareToHistory:
        f.write("<th style=\"border:1px solid black; text-align:center collapse:collapse\">Tendis-"+versionInfo+"测试结果</th>")
        f.write("<th style=\"border:1px solid black; text-align:center collapse:collapse\">较特定版本提升</th>")
    f.write("</tr>")
    f.write("<tr>")
    f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse\">qps</td>")
    f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse\">{0}</td>".format(prettyFormat(qps)))
    if compareToHistory:
        f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse\">{0}</td>".format(prettyFormat(r[4])))
        increase=float(qps)/float(r[4]) - 1.0
        f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse")
        if increase < -float(decreaseLimit)/100.0:
            f.write("; color:red")
        f.write("\">{0}%</td>".format(prettyFormat(increase*100)))
    f.write("</tr>")
    f.write("<tr>")
    f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse\">p50</td>")
    f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse\">{0} ms</td>".format(prettyFormat(p50)))
    if compareToHistory:
        f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse\">{0} ms</td>".format(prettyFormat(r[5])))
        increase=float(p50)/float(r[5]) - 1.0
        f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse")
        if increase > float(decreaseLimitP50)/100.0:
            f.write("; color:red")
        f.write("\">{0}%</td>".format(prettyFormat(increase*100)))
    f.write("</tr>")
    f.write("<tr>")
    f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse\">p99</td>")
    f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse\">{0} ms</td>".format(prettyFormat(p99)))
    if compareToHistory:
        f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse\">{0} ms</td>".format(prettyFormat(r[6])))
        increase=float(p99)/float(r[6]) - 1.0
        f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse")
        if increase > float(decreaseLimitP99)/100.0:
            f.write("; color:red")
        f.write("\">{0}%</td>".format(prettyFormat(increase*100)))
    f.write("</tr>")
    f.write("<tr>")
    f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse\">p100</td>")
    f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse\">{0} ms</td>".format(prettyFormat(p100)))
    if compareToHistory:
        f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse\">{0} ms</td>".format(prettyFormat(r[7])))
        increase=float(p100)/float(r[7]) - 1.0
        f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse")
        if increase > float(decreaseLimitP100)/100.0:
            f.write("; color:red")
        f.write("\">{0}%</td>".format(prettyFormat(increase*100)))
    f.write("</tr>")
    f.write("<tr>")
    f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse\">pavg</td>")
    f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse\">{0} ms</td>".format(prettyFormat(pavg)))
    if compareToHistory:
        f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse\">{0} ms</td>".format(prettyFormat(r[8])))
        increase=float(pavg)/float(r[8]) - 1.0
        f.write("<td style=\"border:1px solid black; text-align:center collapse:collapse")
        if increase > float(decreaseLimitPavg)/100.0:
            f.write("; color:red")
        f.write("\">{0}%</td>".format(prettyFormat(increase*100)))
    f.write("</tr>")
    f.write("</table>")
    f.write("\n")
