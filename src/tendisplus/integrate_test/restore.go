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
)

func testRestore(m1_ip string, m1_port int, s1_ip string, s1_port int,
    s2_ip string, s2_port int, m2_ip string, m2_port int, kvstorecount int) {
    m1 := util.RedisServer{}
    s1 := util.RedisServer{}
    s2 := util.RedisServer{}
    m2 := util.RedisServer{}
    pwd := util.GetCurrentDirectory()
    log.Infof("current pwd:" + pwd)

    cfgArgs := make(map[string]string)
    cfgArgs["kvstorecount"] = strconv.Itoa(kvstorecount)
    cfgArgs["requirepass"] = "tendis+test"
    cfgArgs["masterauth"] = "tendis+test"

    cfgArgs["maxbinlogkeepnum"] = "10000"
    cfgArgs["minbinlogkeepsec"] = "60"
    cfgArgs["truncateBinlogNum"] = "1"

    m1_port = util.FindAvailablePort(m1_port)
    log.Infof("FindAvailablePort:%d", m1_port)
    m1.Init(m1_ip, m1_port, pwd, "m1_")
    if err := m1.Setup(false, &cfgArgs); err != nil {
        log.Fatalf("setup master1 failed:%v", err)
    }

    cfgArgs["maxbinlogkeepnum"] = "10000"
    cfgArgs["minbinlogkeepsec"] = "60"
    s1_port = util.FindAvailablePort(s1_port)
    log.Infof("FindAvailablePort:%d", s1_port)
    s1.Init(s1_ip, s1_port, pwd, "s1_")
    if err := s1.Setup(false, &cfgArgs); err != nil {
        cfgArgs["maxbinlogkeepnum"] = "1"
        cfgArgs["minbinlogkeepsec"] = "0"
        log.Fatalf("setup slave1 failed:%v", err)
    }

    cfgArgs["maxbinlogkeepnum"] = "1"
    cfgArgs["minbinlogkeepsec"] = "0"
    s2_port = util.FindAvailablePort(s2_port)
    log.Infof("FindAvailablePort:%d", s2_port)
    s2.Init(s2_ip, s2_port, pwd, "s2_")
    if err := s2.Setup(false, &cfgArgs); err != nil {
        cfgArgs["maxbinlogkeepnum"] = "1"
        cfgArgs["minbinlogkeepsec"] = "0"
        log.Fatalf("setup slave2 failed:%v", err)
    }

    cfgArgs["maxbinlogkeepnum"] = "10000"
    cfgArgs["minbinlogkeepsec"] = "3600"
    m2_port = util.FindAvailablePort(m2_port)
    log.Infof("FindAvailablePort:%d", m2_port)
    m2.Init(m2_ip, m2_port, pwd, "m2_")
    if err := m2.Setup(false, &cfgArgs); err != nil {
        log.Fatalf("setup master2 failed:%v", err)
    }
    time.Sleep(15 * time.Second)
	clim1 := createClient(&m1)
	clis1 := createClient(&s1)
	clim2 := createClient(&m2)
	clis2 := createClient(&s2)

    slaveof(&m1, &s1)
    waitFullsync(&s1, kvstorecount)

    slaveof(&s1, &s2)
    waitFullsync(&s2, kvstorecount)

    ch := make(chan int)
    util.AddData(&m1, *num1, 0, "aa", ch)
    <-ch
	sha, err := clim1.Cmd("script", "load", "return KEYS[1]").Str()
	if err != nil {
		log.Fatalf("script load on master1 err:%v", err)
	} else if len(sha) != 40 {
		log.Fatalf("wrong sha code length:%d on master1", len(sha))
	}

    waitCatchup(&m1, &s1, kvstorecount)
    waitCatchup(&s1, &s2, kvstorecount)

    backup(&s2, "ckpt", "/tmp/back_test")
    restoreBackup(&m2, "/tmp/back_test")

    var channel chan int = make(chan int)
    go compareInCoroutine(&m1, &s1, channel)
    go compareInCoroutine(&m1, &s2, channel)
    go compareInCoroutine(&m1, &m2, channel)
    <- channel
    <- channel
    <- channel

	s1r, err := clis1.Cmd("script", "exists", sha).Array()
	if err != nil {
		log.Fatalf("script exists on slave1 err:%v", err)
	}
	s1r0, err := s1r[0].Int()
	if err != nil {
		log.Fatalf("script exists on slave1 err:%v, wrong type", err)
	}
	if s1r0 != 1 {
		log.Fatalf("script exists on slave1 err:%v, script not exists", err)
	}

	m2r, err := clim2.Cmd("script", "exists", sha).Array()
	if err != nil {
		log.Fatalf("script exists on master2 err:%v", err)
	}
	m2r0, err := m2r[0].Int()
	if err != nil {
		log.Fatalf("script exists on master2 err:%v, wrong type", err)
	}
	if m2r0 != 1 {
		log.Fatalf("script exists on master2 err:%v, script not exists", err)
	}

	s2r, err := clis2.Cmd("script", "exists", sha).Array()
	if err != nil {
		log.Fatalf("script exists on slave2 err:%v", err)
	}
	s2r0, err := s2r[0].Int()
	if err != nil {
		log.Fatalf("script exists on slave2 err:%v, wrong type", err)
	}
	if s2r0 != 1 {
		log.Fatalf("script exists on slave2 err:%v, script not exists", err)
	}

    addData(&m1, *num2, "bb")
	sha1, err := clim1.Cmd("script", "load", "return KEYS[2]").Str()
	if err != nil {
		log.Fatalf("script load on master1 err:%v", err)
	} else if len(sha1) != 40 {
		log.Fatalf("wrong sha code length:%v on master1", len(sha1))
	}
    addOnekeyEveryStore(&m1, kvstorecount)

    waitCatchup(&m1, &s1, kvstorecount)
    waitCatchup(&s1, &s2, kvstorecount)

    waitDumpBinlog(&s2, kvstorecount)
    flushBinlog(&s2)
    restoreBinlog(&s2, &m2, kvstorecount, math.MaxUint64)
    addOnekeyEveryStore(&m2, kvstorecount)
	sha2, err := clim2.Cmd("script", "load", "return KEYS[2]").Str()
	if err != nil {
		log.Fatalf("script load on master2 err:%v", err)
	} else if sha1 != sha2 {
		log.Fatalf("wrong sha code on m1:%s & m2:%s", sha1, sha2)
	}
    compare(&m1, &m2)

    shutdownServer(&m1, *shutdown, *clear);
    shutdownServer(&s1, *shutdown, *clear);
    shutdownServer(&s2, *shutdown, *clear);
    shutdownServer(&m2, *shutdown, *clear);
}

func main(){
    log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
    flag.Parse()
    // rand.Seed(time.Now().UnixNano())
    testRestore(*m1ip, *m1port, *s1ip, *s1port, *s2ip, *s2port, *m2ip, *m2port, *kvstorecount)
    log.Infof("restore.go passed.")
}
