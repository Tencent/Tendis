// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

package main

import (
    "flag"
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
    arr:=strings.Split(r, "fullsync_succ_times=")
    arr2:=strings.Split(arr[1],",");
    f_times,err:=strconv.Atoi(arr2[0])
    if (f_times != num){
        log.Fatalf("checkFullsyncSuccTimes failed num:%d info:%s", num, r);
    }
    log.Infof("check FullsyncSuccTimes end Path:%s fullsync_succ_times:%d", m.Path, num)
}

func testCluster(clusterIp string, clusterPortStart int, clusterNodeNum int) {
    var servers, predixy, _ = startCluster(clusterIp, clusterPortStart, clusterNodeNum)
    //nodeInfoArray

    // add data
    log.Infof("cluster adddata begin")
    // 100w need about 70 seconds,
    // although redis-benchmark quit about 10 seconds, predixy still need 70 seconds to add data.
    num := 1000000
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

    time.Sleep(32 * time.Second)
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

    time.Sleep(32 * time.Second)
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

    // wait predixy add data end
    time.Sleep(30 * time.Second)

    checkFullsyncSuccTimes(&(*servers)[0], 0)

    // check role
    // old master change to slave
    nodes := getClusterNodes(&(*servers)[0]);
    if !strings.Contains(nodes, "myself,slave") {
        log.Fatalf("check role failed, nodes:%s", nodes)
        return
    }
    // old slave change to master
    nodes = getClusterNodes(&(*servers)[clusterNodeNum]);
    if !strings.Contains(nodes, "myself,master") {
        log.Fatalf("check role failed, nodes:%s", nodes)
        return
    }
    log.Infof("check role end")

    // check incrSync LOG
    checkBinlog(servers, 0, 2) // incrSync 2 times
    checkBinlog(servers, clusterNodeNum, 1)  // incrSync 1 times
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
    flag.Parse()
    testCluster(*clusterIp, 45200, 3)
    log.Infof("clustertestFilover.go passed.")
}
