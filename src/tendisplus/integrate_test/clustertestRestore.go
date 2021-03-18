// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

package main

import (
       "flag"
       "github.com/ngaut/log"
       "math"
       "strconv"
       "tendisplus/integrate_test/util"
       "time"
)

func checkSlotKeyNum(servers *util.RedisServer, slot int, expKeynum int) {
	cli := createClient(servers)
	r, err := cli.Cmd("cluster", "countkeysinslot", slot).Int()
	if err != nil {
		log.Fatalf("cluster countkeysinslot failed:%v %s", err, r)
		return
	}
	if r != expKeynum {
		log.Fatalf("checkSlotKeyNum by countkeysinslot failed, server:%d slot:%d num:%d expKeynum:%d",
			servers.Port, slot, r, expKeynum)
	}
	log.Infof("checkSlotKeyNum by countkeysinslot success, server:%d slot:%d num:%d expKeynum:%d",
		servers.Port, slot, r, expKeynum)
}

func checkSlotEmpty(servers *util.RedisServer, slot int, expectEmpty bool) {
	cli := createClient(servers)
	r, err := cli.Cmd("cluster", "countkeysinslot", slot).Int()
	if err != nil {
		log.Fatalf("cluster countkeysinslot failed:%v %s", err, r)
		return
	}
	if expectEmpty && r != 0 {
		log.Fatalf("checkSlotEmpty by countkeysinslot failed, server:%d slot:%d num:%d expectEmpty:%v",
			servers.Port, slot, r, expectEmpty)
	} else if !expectEmpty && r == 0 {
		log.Fatalf("checkSlotEmpty by countkeysinslot failed, server:%d slot:%d num:%d expectEmpty:%v",
			servers.Port, slot, r, expectEmpty)
	}
	log.Infof("checkSlotEmpty by countkeysinslot success, server:%d slot:%d num:%d expectEmpty:%v",
		servers.Port, slot, r, expectEmpty)
}

func checkDbsize(servers *util.RedisServer, expKeynum int, predixy *util.Predixy) {
	cli := createClient(servers)
	r, err := cli.Cmd("dbsize").Int()
	if err != nil {
		log.Fatalf("cluster countkeysinslot failed:%v %s", err, r)
		return
	}

	if r != expKeynum {
		log.Infof("checkDbsize failed, server:%d num:%d expKeynum:%d, begin checkData...",
			servers.Port, r, expKeynum)
		var channel chan int = make(chan int)
		go checkDataInCoroutine(&predixy.RedisServer, expKeynum, "{12}", "redis-benchmark", channel)
		<-channel
		log.Fatalf("checkDbsize failed")
	}
	log.Infof("checkDbsize success, server:%d num:%d expKeynum:%d",
		servers.Port, r, expKeynum)
}

func testFun1(src_master *util.RedisServer, src_slave *util.RedisServer,
	dst_master *util.RedisServer, dst_slave *util.RedisServer,
	src_restore *util.RedisServer, dst_restore *util.RedisServer,
	predixy *util.Predixy, num int, backup_mode string,
	kvstorecount int) {
	// add data
	log.Infof("cluster adddata begin")
	var channel chan int = make(chan int)
	migSlot := 8373
	go addDataInCoroutine(&predixy.RedisServer, num, "{12}", channel)

	log.Infof("cluster backup begin")
	time.Sleep(1 * time.Second)
	backup(src_slave, backup_mode, "/tmp/back_srcmaster")
	backup(dst_slave, backup_mode, "/tmp/back_dstmaster")

	// migrate
	log.Infof("cluster migrate begin")
	cluster_migrate(src_master, dst_master, migSlot, migSlot)
	var tsBeforeMigrateEnd uint64 = (uint64)(time.Now().UnixNano()) / 1000000

	// wait add data end
	<-channel
	log.Infof("cluster adddata end")

	//expire key
	expireKey(src_master, 100, "{12}")

	// meet new cluster
	log.Infof("cluster meet begin")
	cluster_meet(src_restore, dst_restore)
	time.Sleep(2 * time.Second)

	// addslots new cluster
	log.Infof("cluster addslots begin")
	cluster_addslots(src_restore, 0, 10000)
	cluster_addslots(dst_restore, 10001, 16383)
	time.Sleep(20 * time.Second)

	// restore
	log.Infof("restoreBackup begin")
	restoreBackup(src_restore, "/tmp/back_srcmaster")
	log.Infof("restoreBinlog begin")
	restoreBinlog(src_slave, src_restore, kvstorecount, tsBeforeMigrateEnd)
	restoreBinlogEnd(src_restore, kvstorecount)

	log.Infof("restoreBackup begin")
	restoreBackup(dst_restore, "/tmp/back_dstmaster")
	log.Infof("restoreBinlog begin")
	// dst_restore server need delete chunk keys, because restore before migrate end
	restoreBinlog(dst_slave, dst_restore, kvstorecount, tsBeforeMigrateEnd)
	restoreBinlogEnd(dst_restore, kvstorecount)

	log.Infof("delete dirty data begin")
	{
		cli := createClient(dst_restore)
		if r, err := cli.Cmd("cluster", "clear").Str(); r != ("OK") {
			log.Infof("clear failed:%v %s", err, r)
		}
		cli2 := createClient(src_master)
		if r, err := cli2.Cmd("cluster", "clear").Str(); r != ("OK") {
			log.Infof("clear failed:%v %s", err, r)
		}
	}
	time.Sleep(6 * time.Second)

	log.Infof("checkData begin")
	checkSlotEmpty(src_restore, migSlot, false)
	checkSlotEmpty(dst_restore, migSlot, true)

	checkDbsize(src_master, 0, predixy)
	checkDbsize(src_slave, 0, predixy)
	checkDbsize(dst_master, num, predixy)
	checkDbsize(dst_slave, num, predixy)

	// wait until keys are expired
	time.Sleep(40 * time.Second)
	//checkData(dst_master, 100, "{12}", "redis-benchmark", false)
	checkDbsize(dst_master, num-100, predixy)
	checkDbsize(dst_slave, num-100, predixy)
}

func testFun2(src_master *util.RedisServer, src_slave *util.RedisServer,
	dst_master *util.RedisServer, dst_slave *util.RedisServer,
	src_restore *util.RedisServer, dst_restore *util.RedisServer,
	predixy *util.Predixy, num int, backup_mode string,
	kvstorecount int) {
	// add data
	log.Infof("cluster adddata begin")
	var channel chan int = make(chan int)
	migSlot := 8373
	go addDataInCoroutine(&predixy.RedisServer, num, "{12}", channel)

	// migrate
	log.Infof("cluster migrate begin")
	cluster_migrate(src_master, dst_master, migSlot, migSlot)

	time.Sleep(1 * time.Second)
	log.Infof("cluster backup begin")
	backup(src_slave, backup_mode, "/tmp/back_srcmaster")
	backup(dst_slave, backup_mode, "/tmp/back_dstmaster")

	// wait add data end
	<-channel
	log.Infof("cluster adddata end")

	//waitDumpBinlog(src_slave, kvstorecount)
	//waitDumpBinlog(dst_slave, kvstorecount)
	time.Sleep(10 * time.Second)
	// TOD(takenliu) why dont flush when time pass some seconds ???
	flushBinlog(dst_slave)

	var tsDeleteChunkNotFinish uint64 = math.MaxUint64

	// meet new cluster
	log.Infof("cluster meet begin")
	cluster_meet(src_restore, dst_restore)
	time.Sleep(2 * time.Second)

	// addslots new cluster
	log.Infof("cluster addslots begin")
	cluster_addslots(src_restore, 0, migSlot-1)
	cluster_addslots(src_restore, migSlot+1, 10000)
	cluster_addslots(dst_restore, migSlot, migSlot)
	cluster_addslots(dst_restore, 10001, 16383)
	time.Sleep(10 * time.Second)

	// restore
	log.Infof("restoreBackup begin")
	restoreBackup(src_restore, "/tmp/back_srcmaster")
	log.Infof("restoreBinlog begin")
	// 1.if src_restore backup is doing before migrate end, and restore timestamp is after migrate end,
	//     then need delete chunk keys.
	// 2.if src_restore backup id doing when deleteChunk not finish, the slot not belong to self,
	//     so need delete chunk keys.
	// TODO(takenliu) add gotest for the two different case ???
	restoreBinlog(src_slave, src_restore, kvstorecount, tsDeleteChunkNotFinish)
	restoreBinlogEnd(src_restore, kvstorecount)

	log.Infof("restoreBackup begin")
	restoreBackup(dst_restore, "/tmp/back_dstmaster")
	log.Infof("restoreBinlog begin")
	restoreBinlog(dst_slave, dst_restore, kvstorecount, tsDeleteChunkNotFinish)
	restoreBinlogEnd(dst_restore, kvstorecount)

	time.Sleep(3 * time.Second)

	log.Infof("delete dirty data begin")
	{
		cli := createClient(src_restore)
		if r, err := cli.Cmd("cluster", "clear").Str(); r != ("OK") {
			log.Infof("clear failed:%v %s", err, r)
		}
		cli2 := createClient(src_master)
		if r, err := cli2.Cmd("cluster", "clear").Str(); r != ("OK") {
			log.Infof("clear failed:%v %s", err, r)
		}
	}
	time.Sleep(6 * time.Second)
	checkSlotEmpty(src_restore, migSlot, true)

	checkSlotEmpty(dst_restore, migSlot, false)

	checkDbsize(src_master, 0, predixy)
	checkDbsize(src_slave, 0, predixy)
	checkDbsize(dst_master, num, predixy)
	checkDbsize(dst_slave, num, predixy)
	checkDbsize(src_restore, 0, predixy)
	// truncateBinlogV2 at least keep one binlog
	checkDbsize(dst_restore, num-1, predixy)
}

func testRestore(portStart int, num int, testFun int, commandType string) {
	*benchtype = commandType
	ip := "127.0.0.1"
	kvstorecount := 2
	backup_mode := "copy"

	src_master := util.RedisServer{}
	src_slave := util.RedisServer{}
	dst_master := util.RedisServer{}
	dst_slave := util.RedisServer{}

	src_restore := util.RedisServer{}
	dst_restore := util.RedisServer{}

	pwd := getCurrentDirectory()
	log.Infof("current pwd:" + pwd)

	cfgArgs := make(map[string]string)
	cfgArgs["maxBinlogKeepNum"] = "1"
	cfgArgs["kvstorecount"] = strconv.Itoa(kvstorecount)
	cfgArgs["cluster-enabled"] = "true"
	cfgArgs["pauseTimeIndexMgr"] = "1"
	cfgArgs["rocks.blockcachemb"] = "24"
	cfgArgs["requirepass"] = "tendis+test"
	cfgArgs["masterauth"] = "tendis+test"
	cfgArgs["generalLog"] = "true"

	portStart0 := util.FindAvailablePort(portStart)
	src_master.Init(ip, portStart0, pwd, "src_master_")
	if err := src_master.Setup(*valgrind, &cfgArgs); err != nil {
		log.Fatalf("setup failed:%v", err)
	}
	portStart1 := util.FindAvailablePort(portStart + 1)
	src_slave.Init(ip, portStart1, pwd, "src_slave_")
	if err := src_slave.Setup(*valgrind, &cfgArgs); err != nil {
		log.Fatalf("setup failed:%v", err)
	}
	portStart2 := util.FindAvailablePort(portStart + 2)
	dst_master.Init(ip, portStart2, pwd, "dst_master_")
	if err := dst_master.Setup(*valgrind, &cfgArgs); err != nil {
		log.Fatalf("setup failed:%v", err)
	}
	portStart3 := util.FindAvailablePort(portStart + 3)
	dst_slave.Init(ip, portStart3, pwd, "dst_slave_")
	if err := dst_slave.Setup(*valgrind, &cfgArgs); err != nil {
		log.Fatalf("setup failed:%v", err)
	}

	portStart4 := util.FindAvailablePort(portStart + 4)
	src_restore.Init(ip, portStart4, pwd, "src_restore_")
	if err := src_restore.Setup(*valgrind, &cfgArgs); err != nil {
		log.Fatalf("setup failed:%v", err)
	}
	portStart5 := util.FindAvailablePort(portStart + 5)
	dst_restore.Init(ip, portStart5, pwd, "dst_restore_")
	if err := dst_restore.Setup(*valgrind, &cfgArgs); err != nil {
		log.Fatalf("setup failed:%v", err)
	}
	time.Sleep(20 * time.Second)

	// meet
	log.Infof("cluster meet begin")
	cluster_meet(&src_master, &src_slave)
	cluster_meet(&src_master, &dst_master)
	cluster_meet(&src_master, &dst_slave)
	time.Sleep(2 * time.Second)

	// slaveof
	log.Infof("cluster slaveof begin")
	cluster_slaveof(&src_master, &src_slave)
	cluster_slaveof(&dst_master, &dst_slave)

	// addslots
	log.Infof("cluster addslots begin")
	cluster_addslots(&src_master, 0, 10000)
	cluster_addslots(&dst_master, 10001, 16383)

	time.Sleep(10 * time.Second)

	// start predixy
	portPredixy := util.FindAvailablePort(portStart + 6)
	predixy := util.Predixy{}
	predixy.Init(ip, portPredixy, ip, portStart0, pwd, "predixy_")
	predixyCfgArgs := make(map[string]string)
	if err := predixy.Setup(false, &predixyCfgArgs); err != nil {
		log.Fatalf("setup failed:%v", err)
	}

	time.Sleep(1 * time.Second)

	if testFun == 1 {
		testFun1(&src_master, &src_slave, &dst_master, &dst_slave, &src_restore, &dst_restore, &predixy, num, backup_mode, kvstorecount)
	} else if testFun == 2 {
		testFun2(&src_master, &src_slave, &dst_master, &dst_slave, &src_restore, &dst_restore, &predixy, num, backup_mode, kvstorecount)
	}
	// TODO(takenliu) check dbsize

	shutdownServer(&src_master, *shutdown, *clear)
	shutdownServer(&src_slave, *shutdown, *clear)
	shutdownServer(&dst_master, *shutdown, *clear)
	shutdownServer(&dst_slave, *shutdown, *clear)
	shutdownServer(&src_restore, *shutdown, *clear)
	shutdownServer(&dst_restore, *shutdown, *clear)
	shutdownPredixy(&predixy, *shutdown, *clear)
	log.Infof("testRestore sucess")
}

func main() {
	flag.Parse()
	testRestore(47000, 100000, 1, "set")
	testRestore(47100, 100000, 2, "set")

	// cmd1 := exec.Command("netstat", "-an|grep", "30200")
	// data1, err1 := cmd1.Output()
	// log.Infof("netstat -an |grep 30200 : %s , err: %v", string(data1), err1)
	// cmd11 := exec.Command("netstat", "-an|grep", "30300")
	// data11, err11 := cmd11.Output()
	// log.Infof("netstat -an |grep 30300 : %s , err: %v", string(data11), err11)

	// testRestore(30200, 100000, 1, "sadd")
	// testRestore(30300, 100000, 2, "sadd")
	// log.Infof("clustertestRestore.go passed. command : %s", *benchtype)

	// cmd2 := exec.Command("netstat", "-an|grep", "30400")
	// data2, err2 := cmd2.Output()
	// log.Infof("netstat -an |grep 30400 : %s, err: %v", string(data2), err2)
	// cmd21 := exec.Command("netstat", "-an|grep", "30500")
	// data21, err21 := cmd21.Output()
	// log.Infof("netstat -an |grep 30500 : %s, err: %v", string(data21), err21)

	// testRestore(30400, 100000, 1, "hmset")
	// testRestore(30500, 100000, 2, "hmset")
	// log.Infof("clustertestRestore.go passed. command : %s", *benchtype)

	// cmd3 := exec.Command("netstat", "-an|grep", "59600")
	// data3, err3 := cmd3.Output()
	// log.Infof("netstat -an |grep 59600 : %s, err: %v", string(data3), err3)

	// testRestore(30600, 100000, 1, "rpush")
	// testRestore(30700, 100000, 2, "rpush")
	// log.Infof("clustertestRestore.go passed. command : %s", *benchtype)

	// testRestore(30800, 100000, 1, "zadd")
	// testRestore(30900, 100000, 2, "zadd")
	// log.Infof("clustertestRestore.go passed.")
	log.Infof("clustertestRestore.go passed. command : %s", *benchtype)
}

