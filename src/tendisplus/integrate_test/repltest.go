package main

import (
    "flag"
    "github.com/ngaut/log"
    "tendisplus/integrate_test/util"
    "strconv"
)

func waitFullsyncAndCatchup(m *util.RedisServer, s *util.RedisServer, kvstorecount int, channel chan int) {
    waitFullsync(s, kvstorecount)
    waitCatchup(m, s, kvstorecount)
    channel <- 0
}

func testRestore(m1_ip string, m1_port int, s1_ip string, s1_port int, kvstorecount int) {
    m1 := util.RedisServer{}
    s1 := util.RedisServer{}
    pwd := getCurrentDirectory()
    log.Infof("current pwd:" + pwd)
    m1.Init(m1_ip, m1_port, pwd, "m1_")
    s1.Init(s1_ip, s1_port, pwd, "s1_")

    if *startup == 1 {
        cfgArgs := make(map[string]string)
        cfgArgs["maxBinlogKeepNum"] = strconv.Itoa(*num2 * 5)
        //cfgArgs["maxBinlogKeepNum"] = strconv.Itoa(1)
        cfgArgs["kvstorecount"] = strconv.Itoa(kvstorecount)
        cfgArgs["rocks.blockcachemb"] = strconv.Itoa(1024)
        cfgArgs["requirepass"] = "tendis+test"
    
        if err := m1.Setup(false, &cfgArgs); err != nil {
            log.Fatalf("setup master1 failed:%v", err)
        }
        cfgArgs["masterauth"] = "tendis+test"
        if err := s1.Setup(false, &cfgArgs); err != nil {
            log.Fatalf("setup slave1 failed:%v", err)
        }
    }

    addData(&m1, *num1, *keyprefix1)
    slaveof(&m1, &s1)
    //waitFullsync(&s1, kvstorecount)
    //waitCatchup(&m1, &s1, kvstorecount)

    var channel chan int = make(chan int)
    go waitFullsyncAndCatchup(&m1, &s1, kvstorecount, channel)
    go addDataInCoroutine(&m1, *num2, *keyprefix2, channel)
    <- channel
    <- channel

    waitCatchup(&m1, &s1, kvstorecount)
    if *iscompare == 1 {
        compare(&m1, &s1)
    }

    shutdownServer(&m1, *shutdown, *clear);
    shutdownServer(&s1, *shutdown, *clear);
}

func main(){
    flag.Parse()
    //rand.Seed(time.Now().UTC().UnixNano())
    testRestore(*m1ip, *m1port, *s1ip, *s1port, *kvstorecount)
    log.Infof("repltest.go passed.")
}
