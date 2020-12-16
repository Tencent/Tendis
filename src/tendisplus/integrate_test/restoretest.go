// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

package main

import (
    "flag"
    "github.com/ngaut/log"
    "tendisplus/integrate_test/util"
    "strconv"
    "time"
    "math"
    "strings"
)

func testRestore(m1_ip string, m1_port int, m2_ip string, m2_port int, kvstorecount int, backup_mode string) {
    m1 := util.RedisServer{}
    m2 := util.RedisServer{}
    pwd := getCurrentDirectory()
    log.Infof("current pwd:" + pwd)

    cfgArgs := make(map[string]string)
    cfgArgs["maxBinlogKeepNum"] = "1"
    cfgArgs["kvstorecount"] = strconv.Itoa(kvstorecount)
    cfgArgs["requirepass"] = "tendis+test"
    cfgArgs["masterauth"] = "tendis+test"

    m1_port = util.FindAvailablePort(m1_port)
    log.Infof("FindAvailablePort:%d", m1_port)
    m1.Init(m1_ip, m1_port, pwd, "m1_")
    if err := m1.Setup(false, &cfgArgs); err != nil {
        log.Fatalf("setup master1 failed:%v", err)
    }
    m2_port = util.FindAvailablePort(m2_port)
    log.Infof("FindAvailablePort:%d", m2_port)
    m2.Init(m2_ip, m2_port, pwd, "s1_")
    if err := m2.Setup(false, &cfgArgs); err != nil {
        log.Fatalf("setup master2 failed:%v", err)
    }
    time.Sleep(15 * time.Second)

    // check path cant equal dbPath
    cli := createClient(&m1)
    if r, err := cli.Cmd("backup", m1.Path + "/db", backup_mode).Str();
        err.Error() != ("ERR:4,msg:dir cant be dbPath:" + m1.Path + "/db") {
        log.Fatalf("backup dir cant be dbPath:%v %s", err, r)
        return
    }
    // check path must exist
    if r, err := cli.Cmd("backup", "dir_not_exist", backup_mode).Str();
        err.Error() != ("ERR:4,msg:dir not exist:dir_not_exist") &&
        !strings.Contains(err.Error(), "No such file or directory") {
        log.Fatalf("backup dir must exist:%v %s", err, r)
        return
    }

    addData(&m1, *num1, "aa")
    backup(&m1, backup_mode, "/tmp/back_test")
    restoreBackup(&m2, "/tmp/back_test")

    addData(&m1, *num2, "bb")
    addOnekeyEveryStore(&m1, kvstorecount)
    waitDumpBinlog(&m1, kvstorecount)
    flushBinlog(&m1)
    restoreBinlog(&m1, &m2, kvstorecount, math.MaxUint64)
    addOnekeyEveryStore(&m2, kvstorecount)
    compare(&m1, &m2)

    shutdownServer(&m1, *shutdown, *clear);
    shutdownServer(&m2, *shutdown, *clear);
}

func main(){
    flag.Parse()
    testRestore(*m1ip, *m1port, *m2ip, *m2port, *kvstorecount, "copy")
    // port+100 to avoid TIME_WAIT
    testRestore(*m1ip, *m1port+100, *m2ip, *m2port+100, *kvstorecount, "ckpt")
    log.Infof("restoretest.go passed.")
}