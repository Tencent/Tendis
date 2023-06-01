// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

package main

import (
	"flag"
	"strconv"
	"tendisplus/integrate_test/util"
	"time"

	"github.com/ngaut/log"
)

type paramWrapper struct {
	hardLimit  int
	softLimit  int
	softSecond int
}

func testMemoryLimit() {
	*benchtype = "set"
	ip := "127.0.0.1"
	*kvstorecount = 2
	portStart := 42050

	m1 := util.RedisServer{}

	pwd := util.GetCurrentDirectory()
	log.Infof("current pwd:" + pwd)

	cfgArgs := make(map[string]string)
	cfgArgs["kvstorecount"] = strconv.Itoa(*kvstorecount)
	cfgArgs["requirepass"] = "tendis+test"
	cfgArgs["masterauth"] = "tendis+test"
	cfgArgs["client-output-buffer-limit-normal-hard-mb"] = "2"
	cfgArgs["client-output-buffer-limit-normal-soft-mb"] = "1"
	cfgArgs["client-output-buffer-limit-normal-soft-second"] = "5"

	portStart = util.FindAvailablePort(portStart)
	m1.Init(ip, portStart, pwd, "m1_")
	if err := m1.Setup(*valgrind, &cfgArgs); err != nil {
		log.Fatalf("setup failed:%v", err)
	}

	cli1 := createClient(&m1)

	// write data
	_ = cli1.Cmd("eval", "for i=0,100000 do redis.call('lpush', 'l1', '100000000000000') end", "0")

	pws := []paramWrapper{{2, 1, 5}, {0, 1, 5}, {2, 0, 5}, {0, 0, 5}, {2, 1, 0}, {0, 1, 0}, {2, 0, 0}, {0, 0, 0}}

	for _, pw := range pws {
		_ = cli1.Cmd("config", "set", "client-output-buffer-limit-normal-hard-mb", strconv.Itoa(pw.hardLimit))
		_ = cli1.Cmd("config", "set", "client-output-buffer-limit-normal-soft-mb", strconv.Itoa(pw.softLimit))
		_ = cli1.Cmd("config", "set", "client-output-buffer-limit-normal-soft-second", strconv.Itoa(pw.softSecond))

		// soft limit work correctly
		cliSoft := createClient(&m1)
		// exceed soft limit first time
		if _, err := cliSoft.Cmd("lrange", "l1", "0", "30000").Array(); err != nil {
			log.Fatalf("lrange failed! err:%v", err)
		}
		// no enough time
		if _, err := cliSoft.Cmd("lrange", "l1", "0", "30000").Array(); err != nil {
			log.Fatalf("lrange failed! err:%v", err)
		}
		time.Sleep(5 * time.Second)
		// should be closed
		_, err := cliSoft.Cmd("lrange", "l1", "0", "30000").Array()
		if pw.softLimit != 0 && pw.softSecond != 0 {
			if err == nil {
				log.Fatal("soft limit failed!")
			}
		} else {
			if err != nil {
				log.Fatalf("soft limit failed! err:%v", err)
			}
		}

		// hard limit work correctly
		cliHard := createClient(&m1)
		_, err = cliHard.Cmd("lrange", "l1", "0", "60000").Array()
		if pw.hardLimit != 0 {
			if err == nil {
				log.Fatal("hard limit failed!")
			}
		} else {
			if err != nil {
				log.Fatalf("hard limit failed! err:%v", err)
			}
		}

		// normal command should reset soft limit state
		cliSoft1 := createClient(&m1)
		// exceed soft limit first time
		if _, err := cliSoft1.Cmd("lrange", "l1", "0", "30000").Array(); err != nil {
			log.Fatalf("lrange failed! err:%v", err)
		}
		// run normal command
		if _, err := cliSoft1.Cmd("llen", "l1").Int64(); err != nil {
			log.Fatalf("llen failed! err:%v", err)
		}
		time.Sleep(5 * time.Second)
		// should not be closed
		if _, err := cliSoft1.Cmd("lrange", "l1", "0", "30000").Array(); err != nil {
			log.Fatalf("reset soft limit failed! err:%v", err)
		}
	}

	log.Info("memorylimit.go passed.")

	shutdownServer(&m1, *shutdown, *clear)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	flag.Parse()
	// rand.Seed(time.Now().UnixNano())
	testMemoryLimit()
}
