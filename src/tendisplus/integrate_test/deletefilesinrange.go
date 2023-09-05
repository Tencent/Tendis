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

func getDataCFLevel6FileNum(m *util.RedisServer, storeid int) int {
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
	keynum := 40000

	m1 := util.RedisServer{}

	pwd := util.GetCurrentDirectory()
	log.Infof("current pwd:" + pwd)

	cfgArgs := make(map[string]string)
	cfgArgs["kvstorecount"] = strconv.Itoa(kvStoreCount)
	cfgArgs["cluster-enabled"] = "true"
	cfgArgs["requirepass"] = "tendis+test"
	cfgArgs["masterauth"] = "tendis+test"
	cfgArgs["compactrange-after-deleterange"] = "false"
	cfgArgs["maxbinlogkeepnum"] = "10"
	cfgArgs["rocks.write_buffer_size"] = "1048576"
	cfgArgs["rocks.target_file_size_base"] = "1048576"

	portStart = util.FindAvailablePort(portStart)
	m1.Init(ip, portStart, pwd, "m1_")
	if err := m1.Setup(*valgrind, &cfgArgs); err != nil {
		log.Fatalf("setup failed:%v", err)
	}

	time.Sleep(5 * time.Second)
	// addslots
	log.Infof("cluster addslots begin")
	cluster_addslots(&m1, 0, 16383)

	// cluster state change to ok need delay 5 seconds
	time.Sleep(7 * time.Second)

	// 127.0.0.1:51002> cluster keyslot {12}
	// "8373"
	// 127.0.0.1:51002> cluster keyslot {23}
	// "9671"
	// 127.0.0.1:51002> cluster keyslot {13}
	// "12436"
	// 127.0.0.1:51002> cluster keyslot {14}
	// "115"
	// 127.0.0.1:51002> cluster keyslot {15}
	// "4178"

	// add data and call:
	// step 1: deletefilesinrange data 0 8372 0    to delete tag{15}(slot 4178 on store0)
	// step 2: deletefilesinrange data 0 8372 1    to delete tag{14}(slot 115 on store1)
	// step 3: deletefilesinrange data 8374 16383  to delete tag{13}(slot 12436 on store0) tag{23}(slot 9671 on store1)
	// and check if deletefilesinrange will delete data in wrong store and wrong slot

	// add data
	log.Infof("cluster adddata begin")
	channel := make(chan int)
	util.AddData(&m1, keynum, 0, "{23}", channel)
	util.AddData(&m1, keynum, 0, "{13}", channel)
	util.AddData(&m1, keynum, 0, "{14}", channel)
	util.AddData(&m1, keynum, 0, "{15}", channel)

	cli1 := createClient(&m1)
	r, err := cli1.Cmd("set", "a{12}", "b").Str()
	if err != nil || r != "OK" {
		log.Fatalf("set a{12} b error errmsg:%v ret:%v", err, r)
	}
	r, err = cli1.Cmd("set", "b{12}", "c").Str()
	if err != nil || r != "OK" {
		log.Fatalf("set b{12} c error errmsg:%v ret:%v", err, r)
	}

	for i := 0 ; i < 4; i++ {
		<-channel
	}

	cli1LongTimeout := createClientWithTimeout(&m1, 60) // timeout need be longger
	r, err = cli1LongTimeout.Cmd("compactrange", "data", "0", "16384").Str()
	if err != nil || r != "OK" {
		log.Fatalf("reshape error errmsg:%v ret:%v", err, r)
	}

	beforeStep1Store0L6FileNum := getDataCFLevel6FileNum(&m1, 0)
	beforeStep1Store1L6FileNum := getDataCFLevel6FileNum(&m1, 1)
	beforeStep1AllL6FileNum := beforeStep1Store0L6FileNum + beforeStep1Store1L6FileNum

	r, err = cli1.Cmd("deletefilesinrangeforce", "data", "0", "8372", "0").Str()
	if err != nil || r != "OK" {
		log.Fatalf("deletefilesinrangeforce data 0 8372 0 error errmsg:%v ret:%v", err, r)
	}

	afterStep1Store0L6FileNum := getDataCFLevel6FileNum(&m1, 0)
	afterStep1Store1L6FileNum := getDataCFLevel6FileNum(&m1, 1)
	afterStep1AllL6FileNum := afterStep1Store0L6FileNum + afterStep1Store1L6FileNum

	if beforeStep1Store1L6FileNum != afterStep1Store1L6FileNum {
		log.Fatalf("L6 file number(store1): before:%v after:%v " +
		"deletefilesinrangeforce deleted data on store 1 when specified store 0",
	    beforeStep1Store1L6FileNum, afterStep1Store1L6FileNum)
	}

	r, err = cli1.Cmd("deletefilesinrangeforce", "default", "0", "8372", "1").Str()
	if err != nil || r != "OK" {
		log.Fatalf("deletefilesinrangeforce default 0 8372 1 error errmsg:%v ret:%v", err, r)
	}

	afterStep2Store0L6FileNum := getDataCFLevel6FileNum(&m1, 0)
	afterStep2Store1L6FileNum := getDataCFLevel6FileNum(&m1, 1)
	afterStep2AllL6FileNum := afterStep2Store0L6FileNum + afterStep2Store1L6FileNum

	if afterStep1Store0L6FileNum != afterStep2Store0L6FileNum {
		log.Fatalf("L6 file number(store0): before:%v after:%v" +
		"deletefilesinrangeforce deleted data on store 0 when specified store 1",
	    afterStep1Store0L6FileNum, afterStep2Store0L6FileNum)
	}

	// todo(raffertyyu): try to find out
	// too frequent deletefilesinrange request on rocksdb will cause some requests failed to delete file actually but still return ok.
	time.Sleep(5 * time.Second)

	r, err = cli1.Cmd("deletefilesinrangeforce", "data", "8374", "16383").Str()
	if err != nil || r != "OK" {
		log.Fatalf("deletefilesinrangeforce data 8374 16383 error errmsg:%v ret:%v", err, r)
	}

	afterStep3Store0L6FileNum := getDataCFLevel6FileNum(&m1, 0)
	afterStep3Store1L6FileNum := getDataCFLevel6FileNum(&m1, 1)
	afterStep3AllL6FileNum := afterStep3Store0L6FileNum + afterStep3Store1L6FileNum

	// after step1,2,3 it should remove at least 16-20 sst files.
	if beforeStep1AllL6FileNum - afterStep3AllL6FileNum != 122 {
		log.Fatalf("Wrong result! " +
		           "every step: (num on store0) (num on store1) (num on two stores) " +
		           "before step1(delete on store0): %v %v %v " +
				   "after step1(delete on store0): %v %v %v " +
				   "after step2(delete on store1): %v %v %v " +
				   "after step3(delete on store0 and 1): %v %v %v",
				   beforeStep1Store0L6FileNum, beforeStep1Store1L6FileNum, beforeStep1AllL6FileNum,
				   afterStep1Store0L6FileNum, afterStep1Store1L6FileNum, afterStep1AllL6FileNum,
				   afterStep2Store0L6FileNum, afterStep2Store1L6FileNum, afterStep2AllL6FileNum,
				   afterStep3Store0L6FileNum, afterStep3Store1L6FileNum, afterStep3AllL6FileNum)
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
	if err == nil {
		log.Fatalf("delete on wrong binlog must fail.")
	}
	log.Infof("%v", err)

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
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	flag.Parse()
	// rand.Seed(time.Now().UnixNano())
	testDeleteFilesInRange()
}
