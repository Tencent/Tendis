// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

package main

import (
	"flag"
	"github.com/ngaut/log"
	"strconv"
	"tendisplus/integrate_test/util"
	"time"
	"math"
)

func testAutoGenerateHeartbeatTimestamp() {
	*benchtype = "set"
	ip := "127.0.0.1"
	*kvstorecount = 2
	portStart := 42000
	interval := 1

	m1 := util.RedisServer{}
	m2 := util.RedisServer{}
	m3 := util.RedisServer{}

	pwd := getCurrentDirectory()
	log.Infof("current pwd:" + pwd)

	cfgArgs := make(map[string]string)
	cfgArgs["kvstorecount"] = strconv.Itoa(*kvstorecount)
	cfgArgs["cluster-enabled"] = "true"
	cfgArgs["requirepass"] = "tendis+test"
	cfgArgs["masterauth"] = "tendis+test"
	cfgArgs["compactrange-after-deleterange"] = "false"
	cfgArgs["maxbinlogkeepnum"] = "10"
	cfgArgs["rocks.write_buffer_size"] = "1048576"
	cfgArgs["rocks.target_file_size_base"] = "1048576"
	cfgArgs["generate-heartbeat-binlog-interval"] = strconv.Itoa(interval)
	cfgArgs["cluster-enabled"] = "true"

	portStart = util.FindAvailablePort(portStart)
	m1.Init(ip, portStart, pwd, "m1_")
	if err := m1.Setup(*valgrind, &cfgArgs); err != nil {
		log.Fatalf("setup failed:%v", err)
	}

	portStart = util.FindAvailablePort(portStart)
	m2.Init(ip, portStart, pwd, "m2_")
	if err := m2.Setup(*valgrind, &cfgArgs); err != nil {
		log.Fatalf("setup failed:%v", err)
	}

	portStart = util.FindAvailablePort(portStart)
	m3.Init(ip, portStart, pwd, "m3_")
	if err := m3.Setup(*valgrind, &cfgArgs); err != nil {
		log.Fatalf("setup failed:%v", err)
	}

	cluster_meet(&m1, &m2)
	cluster_meet(&m2, &m3)
	time.Sleep(5 * time.Second)

	// addslots
	log.Infof("cluster addslots begin")
	cluster_addslots(&m1, 0, 16383)
	time.Sleep(5 * time.Second)

	cluster_slaveof(&m1, &m2)
	cluster_slaveof(&m2, &m3)
	time.Sleep(5 * time.Second)

	cli1 := createClient(&m1)
	for i := 0; i < 20; i++ {
		r, err := cli1.Cmd("adminget", "auto_generated_heartbeat", "withttl", "1").Array()
		if err != nil {
			log.Fatalf("adminget auto_generated_heartbeat withttl 1 errmsg:%v ret:%v", err, r)
		}
		// get store 0 metric
		lr, _ := r[0].Array();
		store0ResultDate, err := lr[1].Str();
		if err != nil {
			log.Fatalf("adminget auto_generated_heartbeat withttl 1 errmsg:%v ret:%v", err, r)
		}
		log.Infof("heartbeat date: %v", store0ResultDate)
		store0ResultTS, err := lr[2].Int64();
		if err != nil {
			log.Fatalf("adminget auto_generated_heartbeat withttl 1 errmsg:%v ret:%v", err, r)
		}
		currentTS := time.Now().UnixNano() / int64(time.Millisecond) // get millisecond timestamp
		if math.Abs(float64(currentTS - store0ResultTS)) > float64(2 * interval * 1000) {
			log.Fatalf("heartbeat admincmd hasn't updated for two intervals")
		}
		log.Infof("%v %v", currentTS, store0ResultTS)
		time.Sleep(1 * time.Second)
	}

	cli2 := createClient(&m2)
	for i := 0; i < 20; i++ {
		r, err := cli2.Cmd("adminget", "auto_generated_heartbeat", "withttl", "1").Array()
		if err != nil {
			log.Fatalf("adminget auto_generated_heartbeat withttl 1 errmsg:%v ret:%v", err, r)
		}
		// get store 0 metric
		lr, _ := r[0].Array();
		store0ResultDate, err := lr[1].Str();
		if err != nil {
			log.Fatalf("adminget auto_generated_heartbeat withttl 1 errmsg:%v ret:%v", err, r)
		}
		log.Infof("heartbeat date: %v", store0ResultDate)
		store0ResultTS, err := lr[2].Int64();
		if err != nil {
			log.Fatalf("adminget auto_generated_heartbeat withttl 1 errmsg:%v ret:%v", err, r)
		}
		currentTS := time.Now().UnixNano() / int64(time.Millisecond) // get millisecond timestamp
		if math.Abs(float64(currentTS - store0ResultTS)) > float64(2 * interval * 1000) {
			log.Fatalf("heartbeat admincmd hasn't updated for two intervals")
		}
		log.Infof("%v %v", currentTS, store0ResultTS)
		time.Sleep(1 * time.Second)
	}

	cli3 := createClient(&m3)
	for i := 0; i < 20; i++ {
		r, err := cli3.Cmd("adminget", "auto_generated_heartbeat", "withttl", "1").Array()
		if err != nil {
			log.Fatalf("adminget auto_generated_heartbeat withttl 1 errmsg:%v ret:%v", err, r)
		}
		// get store 0 metric
		lr, _ := r[0].Array();
		store0ResultDate, err := lr[1].Str();
		if err != nil {
			log.Fatalf("adminget auto_generated_heartbeat withttl 1 errmsg:%v ret:%v", err, r)
		}
		log.Infof("heartbeat date: %v", store0ResultDate)
		store0ResultTS, err := lr[2].Int64();
		if err != nil {
			log.Fatalf("adminget auto_generated_heartbeat withttl 1 errmsg:%v ret:%v", err, r)
		}
		currentTS := time.Now().UnixNano() / int64(time.Millisecond) // get millisecond timestamp
		if math.Abs(float64(currentTS - store0ResultTS)) > float64(2 * interval * 1000) {
			log.Fatalf("heartbeat admincmd hasn't updated for two intervals")
		}
		log.Infof("%v %v", currentTS, store0ResultTS)
		time.Sleep(1 * time.Second)
	}

	log.Infof("adminHeartbeat.go passed. command : %s", *benchtype)

	shutdownServer(&m1, *shutdown, *clear)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	flag.Parse()
	testAutoGenerateHeartbeatTimestamp()
}
