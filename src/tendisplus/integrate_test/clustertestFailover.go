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

func testCluster(clusterIp string, clusterPortStart int, clusterNodeNum int,
    failoverQuickly bool) {
    log.Infof("testCluster begin failoverQuickly:%t", failoverQuickly)
    var servers, predixy, _ = startCluster(clusterIp, clusterPortStart, clusterNodeNum)
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
    log.Infof("clustertestFilover.go passed.")
}
