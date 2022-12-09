// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

package main

import (
    "flag"
    "fmt"
    "github.com/ngaut/log"
    "tendisplus/integrate_test/util"
    "os/exec"
    "strconv"
    "strings"
    "time"
)

var (
	mport = flag.Int("masterport", 41001, "master port")
	sport = flag.Int("slaveport", 41102, "slave port")
	tport = flag.Int("targetport", 41203, "target port")
)

func getDbsize(m *util.RedisServer) (int) {
    cli := createClient(m)
    r, err := cli.Cmd("dbsize").Int()
    if err != nil {
        log.Fatalf("cluster countkeysinslot failed:%v %s", err, r)
        return 0
    }
    return r
}

func getClusterNodes(m *util.RedisServer) (string) {
    cli := createClient(m)
    r, err := cli.Cmd("cluster", "nodes").Str()
    if err != nil {
        log.Fatalf("cluster countkeysinslot failed:%v %s", err, r)
        return "failed"
    }
    return r
}

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

func checkFullsyncSuccTimes(m *util.RedisServer, num int) {
    cli := createClient(m)
    r, err := cli.Cmd("info", "replication").Str()
    if err != nil {
        log.Fatalf("cluster countkeysinslot failed:%v %s", err, r)
        return
    }
    // role is slave contain "fullsync_succ_times=*"
    log.Infof("check FullsyncSuccTimes r:%s", r)
    arr:=strings.Split(r, "fullsync_succ_times=")
    arr2:=strings.Split(arr[1],",");
    f_times,err:=strconv.Atoi(arr2[0])
    if (f_times != num){
        log.Fatalf("checkFullsyncSuccTimes failed num:%d info:%s", num, r);
    }
    log.Infof("check FullsyncSuccTimes end Path:%s fullsync_succ_times:%d", m.Path, num)
}

func testClusterManualFailoverIncrSync() {
	cfg := make(map[string]string)
	cfg["aof-enabled"] = "yes"
	cfg["kvStoreCount"] = "2"
	cfg["noexpire"] = "false"
	cfg["generallog"] = "true"
	cfg["requirepass"] = "tendis+test"
	cfg["cluster-enabled"] = "yes"
	cfg["masterauth"] = "tendis+test"

    var servers, predixy, _ = startCluster("127.0.0.1", *mport, 3, map[string]string{"minBinlogKeepSec": "60"})

	master := (*servers)[0]
	// defer shutdownServer(master, *shutdown, *clear)
	slave1 := (*servers)[3]
	// defer shutdownServer(slave1, *shutdown, *clear)
	slave2 := util.StartSingleServer("s_", *sport, &cfg)
	defer shutdownServer(slave2, *shutdown, *clear)

    // add another slave node
	cluster_meet(&master, slave2)
	time.Sleep(5 * time.Second)
	cluster_slaveof(&master, slave2)

	// check cluster online state
	if !cluster_check_state(&master) {
		log.Fatal("cluster state error, online failed")
	}
	log.Debug("cluster state ok, online now!")
	time.Sleep(10 * time.Second)

    // add data in goroutine for a while
    log.Info("start add data in coroutine")
    var channel chan int = make(chan int)
    go addDataInCoroutine(&predixy.RedisServer, 500000, "tag", channel)

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
    <- channel
    log.Info("add data in coroutine end")

	for {
        if (cluster_check_repl_offset(&slave1, &master)) {
            break;
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

func testClusterShutdownFailoverIncrSync() {
    cfg := make(map[string]string)
	cfg["aof-enabled"] = "yes"
	cfg["kvStoreCount"] = "2"
	cfg["noexpire"] = "false"
	cfg["generallog"] = "true"
	cfg["requirepass"] = "tendis+test"
	cfg["cluster-enabled"] = "yes"
	cfg["masterauth"] = "tendis+test"

    var servers, predixy, _ = startCluster("127.0.0.1", *mport, 3, map[string]string{})

	master := (*servers)[0]
	// defer shutdownServer(master, *shutdown, *clear)
	slave1 := (*servers)[3]
	// defer shutdownServer(slave1, *shutdown, *clear)
	slave2 := util.StartSingleServer("s_", *sport, &cfg)
	defer shutdownServer(slave2, *shutdown, *clear)

    arbiter1 := util.StartSingleServer("m_", *mport, &cfg)
    defer shutdownServer(arbiter1, *shutdown, *clear)
    arbiter2 := util.StartSingleServer("m_", *mport, &cfg)
    defer shutdownServer(arbiter2, *shutdown, *clear)

	cluster_meet(&master, &slave1)
	cluster_meet(&master, slave2)
    cluster_meet(&master, arbiter1)
    cluster_meet(&master, arbiter2)
    cluster_set_node_arbiter(arbiter1)
    cluster_set_node_arbiter(arbiter2)
    time.Sleep(5 * time.Second)
	cluster_slaveof(&master, slave2)

	time.Sleep(5 * time.Second)
    if !cluster_check_state(&master) {
		log.Fatal("cluster state error, online failed")
	}
	log.Debug("cluster state ok, online now!")

    // add data in goroutine for a while
    log.Info("start add data in coroutine")
    var channel chan int = make(chan int)
    go addDataInCoroutine(&predixy.RedisServer, 500000, "tag", channel)

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

    for cluster_check_state(&slave1) {
        time.Sleep(10 * time.Millisecond)
    }
    log.Debug("cluster get master failed, offline")
    
    for !cluster_check_state(&slave1) {
        time.Sleep(10 * time.Millisecond)
    }
    log.Debug("cluster vote new master, online again!")

    var new_master *util.RedisServer
    var new_slave *util.RedisServer
    if (cluster_check_is_master(&slave1)) {
        new_master = &slave1
        new_slave = slave2
    }
    if (cluster_check_is_master(slave2)) {
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
    <- channel
    log.Info("add data in coroutine end")

    for {
        if (cluster_check_repl_offset(new_master, new_slave)) {
            break;                    
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

func testCluster(clusterIp string, clusterPortStart int, clusterNodeNum int,
    failoverQuickly bool) {
    log.Infof("testCluster begin failoverQuickly:%t", failoverQuickly)
    var servers, predixy, _ = startCluster(clusterIp, clusterPortStart, clusterNodeNum, map[string]string{})
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
        sleepInter = 8;
    }
    log.Infof("failoverQuickly:%t num:%d sleepInter:%d", failoverQuickly, num, sleepInter)
    var channel chan int = make(chan int)
    go addDataInCoroutine(&predixy.RedisServer, num, "abcd", channel)

    time.Sleep(1 * time.Second)
    checkFullsyncSuccTimes(&(*servers)[clusterNodeNum], 1)

    // failover first time
    log.Infof("cluster failover begin")
    failoverNodeSlave := &(*servers)[clusterNodeNum]
    cliSlave := createClient(failoverNodeSlave)
    if r, err := cliSlave.Cmd("cluster", "failover").Str(); err != nil {
        log.Fatalf("do cluster failover failed:%v", err)
        return
    } else if r != "OK" {
        log.Fatalf("do cluster failover error:%s", r)
        return
    }
    log.Infof("cluster failover sucess,port:%d Path:%v", failoverNodeSlave.Port, failoverNodeSlave.Path)

    time.Sleep(time.Duration(sleepInter) * time.Second)
    checkFullsyncSuccTimes(&(*servers)[0], 0)

    // failover second time
    // need sleep more than 2*clusterNodeTimeout
    failoverNodeMaster := &(*servers)[0]
    cliMaster := createClient(failoverNodeMaster)
    if r, err := cliMaster.Cmd("cluster", "failover").Str(); err != nil {
        log.Fatalf("do cluster failover failed:%v", err)
        return
    } else if r != "OK" {
        log.Fatalf("do cluster failover error:%s", r)
        return
    }
    log.Infof("cluster failover sucess,port:%d Path:%v", failoverNodeMaster.Port, failoverNodeMaster.Path)

    time.Sleep(time.Duration(sleepInter) * time.Second)
    checkFullsyncSuccTimes(&(*servers)[clusterNodeNum], 1)

    // failover third time
    // need sleep more than 2*clusterNodeTimeout
    if r, err := cliSlave.Cmd("cluster", "failover").Str(); err != nil {
        log.Fatalf("do cluster failover failed:%v", err)
        return
    } else if r != "OK" {
        log.Fatalf("do cluster failover error:%s", r)
        return
    }
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
    nodes := getClusterNodes(&(*servers)[0]);
    if !strings.Contains(nodes, oldMasterCurRole) {
        log.Fatalf("check role failed, nodes:%s", nodes)
        return
    }
    nodes = getClusterNodes(&(*servers)[clusterNodeNum]);
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
        size2 := getDbsize(&(*servers)[i + clusterNodeNum])
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

func main(){
    log.SetFlags(log.LstdFlags | log.Lmicroseconds)
    flag.Parse()
    testCluster(*clusterIp, 45200, 3, false)
    testCluster(*clusterIp, 45300, 3, true)
    testClusterManualFailoverIncrSync()
    testClusterShutdownFailoverIncrSync()
    log.Infof("clustertestFilover.go passed.")
}
