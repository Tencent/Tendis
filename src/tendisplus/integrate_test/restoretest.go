package main

import (
    "flag"
    "github.com/ngaut/log"
    "tendisplus/integrate_test/util"
    "strconv"
)

func testRestore(m1_port int, s1_port int, s2_port int, m2_port int, kvstorecount int) {
    m1 := util.RedisServer{}
    m2 := util.RedisServer{}
    pwd := getCurrentDirectory()
    log.Infof("current pwd:" + pwd)
    m1.Init(m1_port, pwd, "m1_")
    m2.Init(m2_port, pwd, "m2_")

    cfgArgs := make(map[string]string)
    cfgArgs["maxBinlogKeepNum"] = "1"
    cfgArgs["kvstorecount"] = strconv.Itoa(kvstorecount)

    if err := m1.Setup(false, &cfgArgs); err != nil {
        log.Fatalf("setup master1 failed:%v", err)
    }
    if err := m2.Setup(false, &cfgArgs); err != nil {
        log.Fatalf("setup master2 failed:%v", err)
    }

    addData(m1_port, *num1, "aa")
    backup(&m1)
    restoreBackup(&m2)

    addData(m1_port,*num2, "bb")
    addOnekeyEveryStore(&m1, kvstorecount)
    waitDumpBinlog(&m1, kvstorecount)
    flushBinlog(&m1)
    restoreBinlog(&m1, &m2, kvstorecount)
    addOnekeyEveryStore(&m2, kvstorecount)
    compare(&m1, &m2)

    shutdownServer(&m1, *shutdown, *clear);
    shutdownServer(&m2, *shutdown, *clear);
}

func main(){
    flag.Parse()
    //rand.Seed(time.Now().UTC().UnixNano())
    testRestore(*m1port, *s1port, *s2port, *m2port, *kvstorecount)
}
