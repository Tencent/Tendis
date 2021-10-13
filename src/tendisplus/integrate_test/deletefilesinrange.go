// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

package main

import (
	"flag"
	"fmt"
	"github.com/ngaut/log"
	"strconv"
	"strings"
	"tendisplus/integrate_test/util"
	"time"
)

func getL6levelstat(m *util.RedisServer, storeid int) int {
	c := createClient(m)
	r, err := c.Cmd("info", "levelstats").Str()
	if err != nil {
		log.Fatalf("info levelstats error errmsg:%v ret:%v", err, r)
	}

	rarr := strings.Split(r, "\r\n")
	for _, lr := range rarr {
		if strings.Contains(lr, fmt.Sprintf("rocksdb%d.level-6", storeid)) {
			llr := strings.Split(lr, "=")
			i, e := strconv.Atoi(llr[len(llr)-1])
			if e != nil {
				log.Fatalf("num string to int err:%v ret:%v", err, r)
			}
			return i
		}
	}
	return -1;
}

func testDeleteFilesInRange() {
	*benchtype = "set"
	ip := "127.0.0.1"
	kvStoreCount := 2
	portStart := 41000
	keynum := 200000

	m1 := util.RedisServer{}

	pwd := getCurrentDirectory()
	log.Infof("current pwd:" + pwd)

	cfgArgs := make(map[string]string)
	cfgArgs["kvstorecount"] = strconv.Itoa(kvStoreCount)
	cfgArgs["cluster-enabled"] = "true"
	cfgArgs["requirepass"] = "tendis+test"
	cfgArgs["masterauth"] = "tendis+test"
	cfgArgs["compactrange-after-deleterange"] = "false"
	cfgArgs["deletefilesinrange-for-binlog"] = "false"
	cfgArgs["maxbinlogkeepnum"] = "10"
	cfgArgs["rocks.write_buffer_size"] = "1048576"
	cfgArgs["rocks.target_file_size_base"] = "1048576"

	portStart = util.FindAvailablePort(portStart)
	m1.Init(ip, portStart, pwd, "m1_")
	if err := m1.Setup(*valgrind, &cfgArgs); err != nil {
		log.Fatalf("setup failed:%v", err)
	}

	// addslots
	log.Infof("cluster addslots begin")
	cluster_addslots(&m1, 0, 16383)

	time.Sleep(5 * time.Second)

	// 127.0.0.1:51002> cluster keyslot {12}
	// "8373"
	// 127.0.0.1:51002> cluster keyslot {23}
	// "9671" > 8373 odd
	// 127.0.0.1:51002> cluster keyslot {13}
	// "12436" > 8373 even
	// 127.0.0.1:51002> cluster keyslot {14}
	// "115" < 8373 odd
	// 127.0.0.1:51002> cluster keyslot {15}
	// "4178" < 8373 even

	// add data and call:
	// deletefilesinrange data 0 8372 0    to delete {15}
	// deletefilesinrange data 0 8372 1    to delete {14}
	// deletefilesinrange data 8374 16383  to delete {13} & {23}
	// and check if deletefilesinrange will delete data in wrong store and wrong slot

	// add data
	log.Infof("cluster adddata begin")
	channel := make(chan int)
	channel1 := make(chan int)
	channel2 := make(chan int)
	channel3 := make(chan int)
	go addDataInCoroutine(&m1, keynum, "{23}", channel)
	go addDataInCoroutine(&m1, keynum, "{13}", channel1)
	go addDataInCoroutine(&m1, keynum, "{14}", channel2)
	go addDataInCoroutine(&m1, keynum, "{15}", channel3)

	cli1 := createClient(&m1)
	r, err := cli1.Cmd("set", "a{12}", "b").Str()
	if err != nil || r != "OK" {
		log.Fatalf("set a{12} b error errmsg:%v ret:%v", err, r)
	}
	r, err = cli1.Cmd("set", "b{12}", "c").Str()
	if err != nil || r != "OK" {
		log.Fatalf("set b{12} c error errmsg:%v ret:%v", err, r)
	}

	<-channel
	<-channel1
	<-channel2
	<-channel3

	n1s0 := getL6levelstat(&m1, 0) // should be 10
	n1s1 := getL6levelstat(&m1, 1) // should be 10
	n1 := n1s0 + n1s1 // 20

	r, err = cli1.Cmd("deletefilesinrangeforce", "data", "0", "8372", "0").Str()
	if err != nil || r != "OK" {
		log.Fatalf("deletefilesinrangeforce data 0 8372 0 error errmsg:%v ret:%v", err, r)
	}

	n2s0 := getL6levelstat(&m1, 0) // should be 6
	n2s1 := getL6levelstat(&m1, 1) // should be 10
	n2 := n2s0 + n2s1 // 16

	if n1s1 != n2s1 {
		log.Fatalf("deletefilesinrangeforce deleted data on store 1 when specified store 0")
	}

	r, err = cli1.Cmd("deletefilesinrangeforce", "default", "0", "8372", "1").Str()
	if err != nil || r != "OK" {
		log.Fatalf("deletefilesinrangeforce default 0 8372 1 error errmsg:%v ret:%v", err, r)
	}

	n3s0 := getL6levelstat(&m1, 0) // should be 6
	n3s1 := getL6levelstat(&m1, 1) // should be 6
	n3 := n3s0 + n3s1 // 12

	if n2s0 != n3s0 {
		log.Fatalf("deletefilesinrangeforce deleted data on store 0 when specified store 1")
	}

	// too frequent deletefilesinrange request on rocksdb will cause some requests failed to delete file actually but still return ok.
	// todo(raffertyyu): try to locale it into rocksdb
	time.Sleep(5 * time.Second)

	r, err = cli1.Cmd("deletefilesinrangeforce", "data", "8374", "16383").Str()
	if err != nil || r != "OK" {
		log.Fatalf("deletefilesinrangeforce data 8374 16383 error errmsg:%v ret:%v", err, r)
	}

	n4s0 := getL6levelstat(&m1, 0) // should be 3
	n4s1 := getL6levelstat(&m1, 1) // should be 3
	n4 := n4s0 + n4s1 // 6

	// n1 - n4 should be 14, [10-18] is ok.
	if n1 - n4 < 10 || n1 - n4 > 18 {
		log.Fatalf("Wrong result! " +
		           "total sst file size: %v %v %v " +
				   "after deletefiles in 0-8372(0): %v %v %v " +
				   "after deletefiles in 0-8372(1): %v %v %v " +
				   "after deletefiles in 8374-16383 %v %v %v",
				   n1s0, n1s1, n1, n2s0, n2s1, n2, n3s0, n3s1, n3, n4s0, n4s1, n4)
	}

	// shouldn't delete data in slot 8373
	r, err = cli1.Cmd("get", "a{12}").Str()
	if err != nil || r != "b" {
		log.Fatalf("get a{12} error errmsg:%v ret:%v", err, r)
	}

	r, err = cli1.Cmd("get", "b{12}").Str()
	if err != nil || r != "c" {
		log.Fatalf("get b{12} error errmsg:%v ret:%v", err, r)
	}

	// deletefilesinrange on binlog
	ri, err := cli1.Cmd("binlogstart", "0").Int()
	if err != nil {
		log.Fatalf("binlogstart 0 error errmsg:%v ret:%v", err, r)
	}

	binlogToDelete := ri - 1;
	// if set wrong end binlogid = minsavedbinlog should return err.
	r, err = cli1.Cmd("deletefilesinrange", "binlog", "0", strconv.Itoa(ri), "0").Str()
	if err != nil {
		log.Infof("%v", err)
	}

	r, err = cli1.Cmd("deletefilesinrange", "binlog", "0", strconv.Itoa(binlogToDelete), "0").Str()
	if err != nil || r != "OK" {
		log.Fatalf(fmt.Sprintf("deletefilesinrange binlog 0 %v", binlogToDelete) + "0 error errmsg:%v ret:%v", err, r)
	}

	ri, err = cli1.Cmd("binlogstart", "1").Int()
	if err != nil {
		log.Fatalf("binlogstart 1 error errmsg:%v ret:%v", err, r)
	}

	binlogToDelete = ri - 1;
	r, err = cli1.Cmd("deletefilesinrange", "binlog", "0", strconv.Itoa(binlogToDelete), "1").Str()
	if err != nil || r != "OK" {
		log.Fatalf(fmt.Sprintf("deletefilesinrange binlog 0 %v", binlogToDelete) + "1 error errmsg:%v ret:%v", err, r)
	}

	// check if deleted wrong binlog pos
	binlogposFromCursor0, err := cli1.Cmd("binlogstart", "0", "fromcursor").Int()
	if err != nil {
		log.Fatalf("binlogstart 0 fromcursor error errmsg:%v ret:%v", err, r)
	}
	binlogposFromMeta0, err := cli1.Cmd("binlogstart", "0").Int()
	if err != nil {
		log.Fatalf("binlogstart 0 error errmsg:%v ret:%v", err, r)
	}

	if binlogposFromCursor0 > binlogposFromMeta0 {
		log.Fatalf("deletefilesinrange on binlog deleted wrong binlog on store 0")
	}

	binlogposFromCursor1, err := cli1.Cmd("binlogstart", "1", "fromcursor").Int()
	if err != nil {
		log.Fatalf("binlogstart 1 fromcursor error errmsg:%v ret:%v", err, r)
	}
	binlogposFromMeta1, err := cli1.Cmd("binlogstart", "1").Int()
	if err != nil {
		log.Fatalf("binlogstart 1 error errmsg:%v ret:%v", err, r)
	}

	if binlogposFromCursor1 > binlogposFromMeta1 {
		log.Fatalf("deletefilesinrange on binlog deleted wrong binlog on store 1")
	}

	log.Infof("deletefilesinrange.go passed. command : %s", *benchtype)

	shutdownServer(&m1, *shutdown, *clear)
}

func main() {
	flag.Parse()
	testDeleteFilesInRange()
}
