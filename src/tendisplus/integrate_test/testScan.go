// Copyright (C) 2025 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

package main

import (
	"flag"
	"integrate_test/util"
	"strconv"
	"time"

	"github.com/ngaut/log"
)

var setCmdList = []string{
	"set", "hset", "sadd", "zadd", "lpush", "hmset",
}

// test ldb_tendis
func testScan(portStart int, num int, commandType string) {
	*util.Optype = commandType
	ip := "127.0.0.1"
	kvstorecount := 2

	pwd := util.GetCurrentDirectory()
	log.Infof("current pwd:" + pwd)

	serv := util.RedisServer{}
	cfgArgs := make(map[string]string)
	cfgArgs["maxBinlogKeepNum"] = "1"
	cfgArgs["kvstorecount"] = strconv.Itoa(kvstorecount)
	cfgArgs["pauseTimeIndexMgr"] = "1"
	cfgArgs["rocks.blockcachemb"] = "24"
	cfgArgs["requirepass"] = "tendis+test"
	cfgArgs["masterauth"] = "tendis+test"
	cfgArgs["generalLog"] = "true"

	portStart0 := util.FindAvailablePort(portStart)
	serv.Init(ip, portStart0, pwd, "serv", util.Standalone)

	if err := serv.Setup(*valgrind, &cfgArgs); err != nil {
		log.Fatalf("setup failed:%v", err)
	}
	time.Sleep(10 * time.Second)

	// add data
	log.Infof("adddata begin")
	cli := util.CreateClientWithGoRedis(&serv, *auth)
	for i := 0; i < len(setCmdList); i++ {
		util.AddTypeDataWithNum(cli, setCmdList[i], 0, num, 20, 1200000, 8, strconv.Itoa(i)+"_")
	}
	log.Infof("adddata end")

	time.Sleep(10 * time.Second)
	log.Infof("scanData begin")
	scanData(&serv, kvstorecount)
	log.Infof("scanData end")
	log.Infof("scanBinlog begin")
	scanBinlog(&serv, kvstorecount)
	log.Infof("scanBinlog end")
	shutdownServer(&serv, *shutdown, *clear)
	log.Infof("scan success")
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	flag.Parse()
	testScan(47000, 1000, "all")
}
