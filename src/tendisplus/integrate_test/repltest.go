package main

import (
    "flag"
    "github.com/ngaut/log"
    "tendisplus/integrate_test/util"
    "strconv"
)

func testRestore(m1_port int, s1_port int, s2_port int, m2_port int, kvstorecount int) {
    m1 := util.RedisServer{}
    s1 := util.RedisServer{}
    pwd := getCurrentDirectory()
    log.Infof("current pwd:" + pwd)
    m1.Init(m1_port, pwd, "m1_")
    s1.Init(s1_port, pwd, "s1_")

    cfgArgs := make(map[string]string)
    cfgArgs["maxBinlogKeepNum"] = strconv.Itoa(*num2 * 5)
    //cfgArgs["maxBinlogKeepNum"] = strconv.Itoa(1)
    cfgArgs["kvstorecount"] = strconv.Itoa(kvstorecount)
    cfgArgs["rocks.blockcachemb"] = strconv.Itoa(1024)

    if err := m1.Setup(false, &cfgArgs); err != nil {
        log.Fatalf("setup master1 failed:%v", err)
    }
    if err := s1.Setup(false, &cfgArgs); err != nil {
        log.Fatalf("setup slave1 failed:%v", err)
    }

    addData(m1_port, *num1, "aa")
    slaveof(&m1, &s1)
    //waitFullsync(&s1, kvstorecount)
    //waitCatchup(&m1, &s1, kvstorecount)

    var channel chan int = make(chan int)
    go waitFullsyncInCoroutine(&s1, kvstorecount, channel)
    go waitCatchupInCoroutine(&m1, &s1, kvstorecount, channel)
    go addDataInCoroutine(m1_port, *num2, "bb", channel)
    <- channel
    <- channel
    <- channel

    waitCatchup(&m1, &s1, kvstorecount)
    compare(&m1, &s1)

    shutdownServer(&m1, *shutdown, *clear);
    shutdownServer(&s1, *shutdown, *clear);
}

func main(){
    flag.Parse()
    //rand.Seed(time.Now().UTC().UnixNano())
    testRestore(*m1port, *s1port, *s2port, *m2port, *kvstorecount)
}
