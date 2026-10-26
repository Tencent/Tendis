// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

package main

import (
	"flag"
	"fmt"
	"integrate_test/util"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/ngaut/log"
)

func checkBinlog(servers *[]util.RedisServer, index int, num int) {
	var expectLog string = "clusterSetMaster incrSync:1"
	logfile := (*servers)[index].Path + "/log/tendisplus.INFO"
	cmd := exec.Command("grep", expectLog, logfile)
	data, err := cmd.Output()
	var strData string = string(data)
	if strings.Count(strData, expectLog) != num || err != nil {
		log.Fatalf("grep failed, logfile:%s num:%d data:%s err:%v", logfile, num, data, err)
		return
	}
	log.Infof("check incrSync log end")
}

func testClusterManualFailoverIncrSync(mport int) {
	cfg := make(map[string]string)
	cfg["aof-enabled"] = "yes"
	cfg["kvStoreCount"] = "2"
	cfg["noexpire"] = "false"
	cfg["generallog"] = "true"
	cfg["requirepass"] = "tendis+test"
	cfg["cluster-enabled"] = "yes"
	cfg["masterauth"] = "tendis+test"

	sport := mport + 6
	var servers, predixy, _ = startCluster("127.0.0.1", mport, 3, map[string]string{"minBinlogKeepSec": "60"}, util.GetCurrentDirectory(), "")

	master := (*servers)[0]
	// defer shutdownServer(master, *shutdown, *clear)
	slave1 := (*servers)[3]
	// defer shutdownServer(slave1, *shutdown, *clear)
	slave2 := util.StartSingleServer("s_", sport, &cfg)
	defer shutdownServer(slave2, *shutdown, *clear)

	// add another slave node
	cluster_meet(&master, slave2)
	waitMeetEndSingle(*slave2, 7)
	waitMeetEnd(servers, 7)
	cluster_slaveof(&master, slave2)

	// check cluster online state
	if !cluster_check_state_continuous(&master) {
		log.Fatal("cluster state error, online failed")
	}
	log.Debug("cluster state ok, online now!")
	time.Sleep(10 * time.Second)

	// add data in goroutine for a while
	log.Info("start add data in coroutine")
	var channel chan int = make(chan int)
	go util.AddDataWithBenchmarkInCo(&predixy.RedisServer, *auth, 500000, "tag", "set", channel)

	// check master sync_full and partial_sync times
	if !cluster_check_sync_full(&master, 4) {
		log.Fatal("cluster check sync_full error")
	}
	log.Debug("cluster check sync_full ok")

	if !cluster_check_sync_partial_ok(&master, 4) {
		log.Fatalf("cluster check sync_partial_ok error")
	}
	log.Debug("cluster check sync_partial_ok ok")

	// slave1 MANUAL FAILOVER
	cluster_manual_failover(&slave1)

	time.Sleep(15 * time.Second)
	// check master sync_full and partial_sync times again
	if !cluster_check_sync_full(&slave1, 0) {
		log.Fatal("cluster check sync_full error")
	}
	log.Debug("after manual failover cluster check sync_full ok")

	if !cluster_check_sync_partial_ok(&slave1, 4) {
		log.Fatalf("cluster check sync_partial_ok error")
	}
	log.Debug("after manual failover cluster check sync_partial_ok ok")

	// wait add data end by channel write
	<-channel
	log.Info("add data in coroutine end")

	for {
		if cluster_check_repl_offset(&slave1, &master) {
			break
		} else {
			log.Info("cluster check repl offset not equal, wait")
			time.Sleep(1 * time.Second)
		}
	}

	// compare instance data
	util.CompareClusterDataWithAuth(slave1.Addr(), *auth, master.Addr(), *auth, 2, true)
	util.CompareClusterDataWithAuth(slave1.Addr(), *auth, slave2.Addr(), *auth, 2, true)
	stopCluster(servers, 3, predixy)
	log.Debug("test manualFailover incr-sync ok!")
}

func testClusterShutdownFailoverIncrSync(mport int) {
	cfg := make(map[string]string)
	cfg["aof-enabled"] = "yes"
	cfg["kvStoreCount"] = "2"
	cfg["noexpire"] = "false"
	cfg["generallog"] = "true"
	cfg["requirepass"] = "tendis+test"
	cfg["cluster-enabled"] = "yes"
	cfg["masterauth"] = "tendis+test"
	cfg["minBinlogKeepSec"] = "3600"

	sport := mport + 6
	var servers, predixy, _ = startCluster("127.0.0.1", mport, 3, map[string]string{"minBinlogKeepSec": "3600"}, util.GetCurrentDirectory(), "")

	master := (*servers)[0]
	// defer shutdownServer(master, *shutdown, *clear)
	slave1 := (*servers)[3]
	// defer shutdownServer(slave1, *shutdown, *clear)
	slave2 := util.StartSingleServer("s_", sport, &cfg)
	defer shutdownServer(slave2, *shutdown, *clear)

	arbiter1 := util.StartSingleServer("m_", mport+7, &cfg)
	defer shutdownServer(arbiter1, *shutdown, *clear)
	arbiter2 := util.StartSingleServer("m_", mport+8, &cfg)
	defer shutdownServer(arbiter2, *shutdown, *clear)

	cluster_meet(&master, &slave1)
	cluster_meet(&master, slave2)
	cluster_meet(&master, arbiter1)
	cluster_meet(&master, arbiter2)
	cluster_set_node_arbiter(arbiter1)
	cluster_set_node_arbiter(arbiter2)
	time.Sleep(5 * time.Second)

	waitMeetEndSingle(*slave2, 9)
	waitMeetEndSingle(*arbiter1, 9)
	waitMeetEndSingle(*arbiter2, 9)
	waitMeetEnd(servers, 9)

	cluster_slaveof(&master, slave2)

	time.Sleep(5 * time.Second)
	if !cluster_check_state_continuous(&master) {
		log.Fatal("cluster state error, online failed")
	}
	log.Debug("cluster state ok, online now!")

	// add data in goroutine for a while
	log.Info("start add data in coroutine")
	var channel chan int = make(chan int)
	go util.AddDataWithBenchmarkInCo(&predixy.RedisServer, *auth, 500000, "tag", "set", channel)

	time.Sleep(5 * time.Second)

	// check master sync_full and partial_sync times
	if !cluster_check_sync_full(&master, 4) {
		log.Fatal("cluster check sync_full error")
	}
	log.Debug("cluster check sync_full ok")

	if !cluster_check_sync_partial_ok(&master, 4) {
		log.Fatalf("cluster check sync_partial_ok error")
	}
	log.Debug("cluster check sync_partial_ok ok")

	log.Debugf("shutdown master node[%s]", master.Addr())
	shutdownServer(&master, *shutdown, *clear)

	for cluster_check_state_once(&slave1) {
		time.Sleep(10 * time.Millisecond)
	}
	log.Debug("cluster get master failed, offline")

	for !cluster_check_state_once(&slave1) {
		time.Sleep(10 * time.Millisecond)
	}
	log.Debug("cluster vote new master, online again!")

	var new_master *util.RedisServer
	var new_slave *util.RedisServer
	if cluster_check_is_master(&slave1) {
		new_master = &slave1
		new_slave = slave2
	}
	if cluster_check_is_master(slave2) {
		new_master = slave2
		new_slave = &slave1
	}
	log.Infof("vote new master[%s]", new_master.Addr())

	time.Sleep(15 * time.Second)
	// check master sync_full and partial_sync times again
	if !cluster_check_sync_full(new_master, 0) {
		log.Fatal("cluster check sync_full error")
	}
	log.Debug("after shutdown failover cluster check sync_full ok")

	if !cluster_check_sync_partial_ok(new_master, 2) {
		log.Fatalf("cluster check sync_partial_ok error")
	}
	log.Debug("after shutdown failover cluster check sync_partial_ok ok")

	// wait add data end by channel write
	<-channel
	log.Info("add data in coroutine end")

	for {
		if cluster_check_repl_offset(new_master, new_slave) {
			break
		} else {
			log.Info("cluster check repl offset not equal, wait")
			time.Sleep(1 * time.Second)
		}
	}

	// compare instance data
	util.CompareClusterDataWithAuth(slave1.Addr(), *auth, slave2.Addr(), *auth, 2, true)

	shutdownPredixy(predixy, *shutdown, *clear)
	shutdownServer(&(*servers)[1], *shutdown, *clear)
	shutdownServer(&(*servers)[2], *shutdown, *clear)
	shutdownServer(&(*servers)[4], *shutdown, *clear)
	shutdownServer(&(*servers)[5], *shutdown, *clear)
	log.Debugf("test shutdownFailover incr-sync ok!")
}

func testClusterFailover(clusterIp string, clusterPortStart int, clusterNodeNum int,
	failoverQuickly bool) {
	log.Infof("testClusterFailover begin failoverQuickly:%t", failoverQuickly)
	var servers, predixy, _ = startCluster(clusterIp, clusterPortStart, clusterNodeNum, map[string]string{}, util.GetCurrentDirectory(), "")
	//nodeInfoArray

	// add data
	log.Infof("cluster adddata begin")
	// 100w need about 70 seconds,
	// although redis-benchmark quit about 10 seconds, predixy still need 70 seconds to add data.
	num := 1000000
	sleepInter := 32 // more than clusterNodeTimeout*2
	if failoverQuickly {
		num = 10000
		// if sleep less than clusterNodeTimeout/2, _mfMasterOffset maybe 0
		sleepInter = 8
	}
	log.Infof("failoverQuickly:%t num:%d sleepInter:%d", failoverQuickly, num, sleepInter)
	var channel chan int = make(chan int)
	go util.AddDataWithBenchmarkInCo(&predixy.RedisServer, *auth, num, "abcd", "set", channel)

	time.Sleep(1 * time.Second)
	checkFullsyncSuccTimes(&(*servers)[clusterNodeNum], 1)

	// failover first time
	log.Infof("cluster failover begin")
	failoverNodeSlave := &(*servers)[clusterNodeNum]
	cluster_manual_failover(failoverNodeSlave)
	log.Infof("cluster failover sucess,port:%d Path:%v", failoverNodeSlave.Port, failoverNodeSlave.Path)

	time.Sleep(time.Duration(sleepInter) * time.Second)
	checkFullsyncSuccTimes(&(*servers)[0], 0)

	// failover second time
	// need sleep more than 2*clusterNodeTimeout
	failoverNodeMaster := &(*servers)[0]
	cluster_manual_failover(failoverNodeMaster)
	log.Infof("cluster failover sucess,port:%d Path:%v", failoverNodeMaster.Port, failoverNodeMaster.Path)

	time.Sleep(time.Duration(sleepInter) * time.Second)
	checkFullsyncSuccTimes(&(*servers)[clusterNodeNum], 1)

	// failover third time
	// need sleep more than 2*clusterNodeTimeout
	cluster_manual_failover(failoverNodeSlave)
	log.Infof("cluster failover sucess,port:%d Path:%v", failoverNodeSlave.Port, failoverNodeSlave.Path)

	// wait redis-benchmark add data end
	<-channel
	log.Infof("cluster adddata end")

	// when do cluster failover, old master change to slave, it shouldn't close the clients connection
	// so redis-benchmark shouldn't print log: "Error from server: ERR server connection close"
	logFilePath := fmt.Sprintf("benchmark_%d.log", predixy.RedisServer.Port)
	log.Infof("check redis-benchmark log file: %s", logFilePath)
	//logcontent := "Error from server: ERR server connection close"
	logcontent := "Err|ERR"
	cmd := fmt.Sprintf("grep -E \"%s\" %s|wc -l", logcontent, logFilePath)
	out, err := exec.Command("sh", "-c", cmd).CombinedOutput()
	if err != nil {
		log.Fatalf("grep %s failed %v", logFilePath, err)
		return
	}
	log.Infof("logcontent: %s", string(out))
	if string(out) != "0\n" {
		log.Fatalf("%s logcontent: %s", logFilePath, string(out))
		return
	}

	// wait predixy add data end
	time.Sleep(50 * time.Second)

	oldMasterCurRole := "myself,slave"
	oldSlaveCurRole := "myself,master"
	oldMasterIncrSyncTimes := 2
	oldSlaveIncrSyncTimes := 1
	if !failoverQuickly {
		// need role be slave
		checkFullsyncSuccTimes(&(*servers)[0], 0)
	} else {
		// sleep less than 2*clusterNodeTimeout,so the third time failover will failed.
		// "Failover auth denied to * can't vote about this master before * milliseconds"
		oldMasterCurRole = "myself,master"
		oldSlaveCurRole = "myself,slave"
		oldMasterIncrSyncTimes = 1
	}
	log.Infof(`failoverQuickly:%d oldMasterCurRole:%s oldSlaveCurRole:%s
        oldMasterIncrSyncTimes:%d oldSlaveIncrSyncTimes:%d`,
		failoverQuickly, oldMasterCurRole, oldSlaveCurRole,
		oldMasterIncrSyncTimes, oldSlaveIncrSyncTimes)

	// check role
	nodes := getClusterNodes(&(*servers)[0])
	if !strings.Contains(nodes, oldMasterCurRole) {
		log.Fatalf("check role failed, nodes:%s", nodes)
		return
	}
	nodes = getClusterNodes(&(*servers)[clusterNodeNum])
	if !strings.Contains(nodes, oldSlaveCurRole) {
		log.Fatalf("check role failed, nodes:%s", nodes)
		return
	}
	log.Infof("check role end")

	// check incrSync LOG
	checkBinlog(servers, 0, oldMasterIncrSyncTimes)
	checkBinlog(servers, clusterNodeNum, oldSlaveIncrSyncTimes)
	log.Infof("check incrSync log end")

	// check dbsize
	totalsize := 0
	for i := 0; i < clusterNodeNum; i++ {
		totalsize += getDbsize(&(*servers)[i])
		// check master and slave dbsize is the same
		size1 := getDbsize(&(*servers)[i])
		size2 := getDbsize(&(*servers)[i+clusterNodeNum])
		if size1 != size2 {
			log.Fatalf("checkDbsize failed i:%d size1:%d size2:%d", i, size1, size2)
			return
		}
	}
	// NOTE: when doing failover, cluster state will be failed,
	//   so totalsize should be smaller than num
	log.Infof("should totalsize:%d ~= num:%d", totalsize, num)

	stopCluster(servers, clusterNodeNum, predixy)
}

func testClusterFailoverSlaveChange(clusterIp string, clusterPortStart int, clusterNodeNum int) {
	var servers, predixy, _ = startCluster(clusterIp, clusterPortStart, clusterNodeNum, map[string]string{}, util.GetCurrentDirectory(), "")
	extraServers := []util.RedisServer{}
	server := util.RedisServer{}
	pwd := util.GetCurrentDirectory()
	for i := 0; i < clusterNodeNum*2; i++ {
		port := util.FindAvailablePort(clusterPortStart+clusterNodeNum*2+i, true)
		log.Infof("start server i:%d port:%d", i, port)
		server.Init(clusterIp, port, pwd, "m"+strconv.Itoa(i)+"_", util.Cluster)
		cfgArgs := make(map[string]string)
		cfgArgs["aof-enabled"] = "yes"
		cfgArgs["kvStoreCount"] = "2"
		cfgArgs["requirepass"] = "tendis+test"
		cfgArgs["cluster-enabled"] = "yes"
		cfgArgs["masterauth"] = "tendis+test"

		if err := server.Setup(false, &cfgArgs); err != nil {
			log.Fatalf("setup failed,port:%s err:%v", port, err)
		}
		extraServers = append(extraServers, server)
	}
	var channel chan int = make(chan int)
	go util.AddDataWithBenchmarkInCo(&predixy.RedisServer, *auth, 100, "tag", "set", channel)
	<-channel

	for i := range extraServers {
		cluster_meet(&(*servers)[0], &extraServers[i])
	}
	waitMeetEnd(&extraServers, clusterNodeNum*4)

	for i := range extraServers {
		masterNodeNum := i % clusterNodeNum
		replicateof(&(*servers)[masterNodeNum], &extraServers[i])
	}
	time.Sleep(15 * time.Second)

	for i := clusterNodeNum; i < clusterNodeNum*2; i++ {
		cluster_manual_failover(&extraServers[i])
	}
	time.Sleep(45 * time.Second)

	for i := clusterNodeNum; i < clusterNodeNum*2; i++ {
		if !isMaster(&extraServers[i]) {
			log.Fatalf("check role failed, %s should be master", getNodeName(&extraServers[i]))
			return
		}
	}

	for i := 0; i < clusterNodeNum; i++ {
		if !checkisMaster(&extraServers[i+clusterNodeNum], &extraServers[i]) {
			log.Fatalf("check role failed, %s should be %s slave", getNodeName(&extraServers[i]), getNodeName(&extraServers[i+clusterNodeNum]))
		}
	}

	for i := 0; i < clusterNodeNum*2; i++ {
		masterNodeNum := i%clusterNodeNum + clusterNodeNum
		if !checkisMaster(&extraServers[masterNodeNum], &(*servers)[i]) {
			log.Fatalf("check role failed, %s should be %s slave", getNodeName(&(*servers)[i]), getNodeName(&extraServers[masterNodeNum]))
		}
	}

	stopCluster(servers, clusterNodeNum, predixy)
	for i := 0; i < clusterNodeNum*2; i++ {
		shutdownServer(&extraServers[i], *shutdown, *clear)
	}
}

// One master per shard, each with one slave. Each round adds two slaves per
// shard and promotes the first. The old master, the previous slave and the
// other new slave must follow it. When another round remains, the old master
// and the previous slave are removed.
func testFailoverSlavesFollow(clusterIp string, portStart int) {
	const shardNum = 18
	const roundNum = 3
	const kvCount = 2
	const keyNum = 1000
	conf := map[string]string{
		"slave-migrate-enabled": "yes",
	}
	conf["kvstorecount"] = strconv.Itoa(kvCount)

	servers, predixy, _ := startCluster(
		clusterIp, portStart, shardNum, conf, util.GetCurrentDirectory(), "")
	defer shutdownPredixy(predixy, *shutdown, *clear)

	masters := make([]*util.RedisServer, shardNum)
	slaves := make([]*util.RedisServer, shardNum)
	var alive []*util.RedisServer
	for i := 0; i < shardNum; i++ {
		masters[i] = &(*servers)[i]
		slaves[i] = &(*servers)[shardNum+i]
		alive = append(alive, masters[i], slaves[i])
	}
	defer func() {
		for _, srv := range alive {
			shutdownServer(srv, *shutdown, *clear)
		}
	}()

	nextPort := portStart + shardNum*2
	for round := 1; round <= roundNum; round++ {
		tag := fmt.Sprintf("round%d", round)
		first := make([]*util.RedisServer, shardNum)
		second := make([]*util.RedisServer, shardNum)

		// Add two new slaves for every shard.
		for i := 0; i < shardNum; i++ {
			first[i] = startClusterNode(clusterIp, &nextPort, util.GetCurrentDirectory(), fmt.Sprintf("%s_a%d_", tag, i), "", conf)
			alive = append(alive, first[i])
		}
		for i := 0; i < shardNum; i++ {
			second[i] = startClusterNode(clusterIp, &nextPort, util.GetCurrentDirectory(), fmt.Sprintf("%s_b%d_", tag, i), "", conf)
			alive = append(alive, second[i])
		}

		// Join the new nodes to the cluster and wait for all views to converge.
		for i := 0; i < shardNum; i++ {
			cluster_meet(masters[0], first[i])
			cluster_meet(masters[0], second[i])
		}
		servers := make([]util.RedisServer, len(alive))
		for i, srv := range alive {
			servers[i] = *srv
		}
		waitMeetEnd(&servers, len(servers))

		// Make both new nodes replicate their shard master.
		for i := 0; i < shardNum; i++ {
			replicateof(masters[i], first[i])
			replicateof(masters[i], second[i])
		}

		// Write data. Each master must have exactly these three slaves,
		// then the first new slave of every shard must catch up.
		ch := make(chan int)
		go util.AddDataInCoroutine(&predixy.RedisServer, *auth, keyNum, tag, ch, "set")
		<-ch
		for i := 0; i < shardNum; i++ {
			curSlaves := []*util.RedisServer{slaves[i], first[i], second[i]}
			waitSlavesKnown(alive, masters[i], curSlaves, 60*time.Second)
		}
		for i := 0; i < shardNum; i++ {
			waitCatchup(masters[i], first[i], kvCount)
		}
		// Fullsync clears lastBinlogTs. Wait for the first incremental heartbeat
		// before failover, or the lag check rejects the slave.
		time.Sleep(5 * time.Second)

		// Promote the first new slave of every shard.
		var lastPromoted time.Time
		for i := 0; i < shardNum; i++ {
			log.Infof("%s promote shard:%d port:%d", tag, i, first[i].Port)
			begin := time.Now()
			cluster_manual_failover(first[i])
			deadline := time.Now().Add(10 * time.Second)
			for !isMyselfMasterWithSlots(first[i]) {
				if time.Now().After(deadline) {
					log.Fatalf("%s promote shard:%d port:%d did not become master in %s",
						tag, i, first[i].Port, time.Since(begin))
				}
				time.Sleep(50 * time.Millisecond)
			}
			lastPromoted = time.Now()
			log.Infof("%s promote shard:%d port:%d became master in %s",
				tag, i, first[i].Port, time.Since(begin))
		}

		// Migration starts only after a master stays orphaned for
		// CLUSTER_SLAVE_MIGRATION_DELAY (5s), and the next cron does the move.
		// Wait out 10s from the last promotion, then require exactly these slaves.
		if wait := 10*time.Second - time.Since(lastPromoted); wait > 0 {
			time.Sleep(wait)
		}
		for i := 0; i < shardNum; i++ {
			curSlaves := []*util.RedisServer{masters[i], slaves[i], second[i]}
			waitSlavesKnown(alive, first[i], curSlaves, 60*time.Second)
		}
		if round == roundNum {
			// Each round adds keyNum keys and they stay after failover,
			// so the final masters must hold every round.
			total := 0
			for i := 0; i < shardNum; i++ {
				total += getDbsize(first[i])
			}
			if total != roundNum*keyNum {
				log.Fatalf("dbsize %d != %d", total, roundNum*keyNum)
			}
			break
		}

		// Remove the previous generation before starting the next round.
		gone := append([]*util.RedisServer{}, masters...)
		gone = append(gone, slaves...)
		goneNames := make([]string, len(gone))
		for i, srv := range gone {
			goneNames[i] = getNodeName(srv)
		}
		left := make([]*util.RedisServer, 0, len(alive))
		goneSet := map[*util.RedisServer]bool{}
		for _, srv := range gone {
			goneSet[srv] = true
			shutdownServer(srv, *shutdown, *clear)
		}
		for _, srv := range alive {
			if !goneSet[srv] {
				left = append(left, srv)
			}
		}
		alive = left
		for _, srv := range alive {
			for _, name := range goneNames {
				cluster_forget(srv, name)
			}
		}
		masters = first
		slaves = second
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	flag.Parse()
	// rand.Seed(time.Now().UnixNano())
	testClusterFailover(*clusterIp, 45200, 3, false)
	testClusterFailover(*clusterIp, 45220, 3, true)
	testClusterFailoverSlaveChange(*clusterIp, 45260, 3)
	testClusterManualFailoverIncrSync(45280)
	testClusterShutdownFailoverIncrSync(45300)
	testFailoverSlavesFollow(*clusterIp, 46000)
	log.Infof("clustertestFailover.go passed.")
}
