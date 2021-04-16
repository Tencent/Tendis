// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

package main

import (
    "fmt"
    "github.com/ngaut/log"
    "tendisplus/integrate_test/util"
    "strings"
    "strconv"
    "time"
)

func cluster_meet(m *util.RedisServer, s *util.RedisServer) {
    cli := createClient(m)

    if r, err := cli.Cmd("cluster", "meet", s.Ip, s.Port).Str(); err != nil {
        log.Fatalf("do meet failed:%v", err)
        return
    } else if r != "OK" {
        log.Fatalf("do meet error:%s", r)
        return
    }
    log.Infof("meet sucess,mport:%d sport:%d" , m.Port, s.Port)
}

func getNodeName(m *util.RedisServer) string {
    cli := createClient(m)
    output, err := cli.Cmd("cluster", "myid").Str();
    if err != nil {
        fmt.Print(err)
    }
    log.Infof("getNodeName sucess. %s:%d nodename:%s", m.Ip, m.Port, output)
    return strings.Trim(string(output), "\n");
}

func cluster_slaveof(m *util.RedisServer, s *util.RedisServer) {
    cli := createClient(s)
    masterNodeName := getNodeName(m)
    if r, err := cli.Cmd("cluster", "replicate", masterNodeName).Str(); err != nil {
        log.Fatalf("do replicate failed:%v", err)
        return
    } else if r != "OK" {
        log.Fatalf("do replicate error:%s", r)
        return
    }
    log.Infof("replicate sucess,mport:%d sport:%d" , m.Port, s.Port)
}

func cluster_addslots(m *util.RedisServer, startSlot int, endSlot int) {
    cli := createClient(m)
    var slots string
    if startSlot == endSlot {
        slots = strconv.Itoa(startSlot)
    } else {
        slots = "{" + strconv.Itoa(startSlot) + ".." + strconv.Itoa(endSlot) + "}"
    }
    if r, err := cli.Cmd("cluster", "addslots", slots).Str(); err != nil {
        log.Fatalf("do addslots failed:%v", err)
        return
    } else if r != "OK" {
        log.Fatalf("do addslots error:%s", r)
        return
    }
    log.Infof("addslots sucess,mport:%d startSlot:%d endSlot:%d" , m.Port, startSlot, endSlot)
}

func cluster_migrate(src *util.RedisServer, dst *util.RedisServer, startSlot int, endSlot int) {
    cli := createClient(dst)
    srcNodeName := getNodeName(src)
    var slots []int
    for j := startSlot; j <= endSlot; j++ {
        slots = append(slots, j)
    }
    if r, err := cli.Cmd("cluster", "setslot", "importing", srcNodeName, slots).Str(); err != nil {
        log.Fatalf("do migrate failed:%v", err)
        return
    } else if err != nil {
        log.Fatalf("do migrate error:%s", r)
        return
    }
    log.Infof("migrate sucess,srcport:%d dstport:%d startSlot:%d endSlot:%d" , src.Port, dst.Port, startSlot, endSlot)
}

var (
    CLUSTER_SLOTS = 16384
)
type NodeInfo struct {
    index int
    startSlot int
    endSlot int
    migrateStartSlot int
    migrateEndSlot int
}

func startCluster(clusterIp string, clusterPortStart int, clusterNodeNum int) (*[]util.RedisServer, *util.Predixy, *[]NodeInfo) {
    var nodeInfoArray []NodeInfo
    for i := 0; i <= clusterNodeNum; i++ {
        var startSlot = CLUSTER_SLOTS / clusterNodeNum * i;
        var endSlot = startSlot + CLUSTER_SLOTS / clusterNodeNum - 1;
        if i == (clusterNodeNum - 1) {
            endSlot = CLUSTER_SLOTS - 1;
        }
        nodeInfoArray = append(nodeInfoArray,
            NodeInfo{i, startSlot, endSlot, 0, 0})
    }

    pwd := getCurrentDirectory()
    log.Infof("current pwd:" + pwd)
    kvstorecount := 2

    var servers []util.RedisServer

    // start server
    log.Infof("start servers clusterNodeNum:%d", clusterNodeNum)
    for i := 0; i < clusterNodeNum * 2; i++ {
        server := util.RedisServer{}
        port := util.FindAvailablePort(clusterPortStart)
        log.Infof("start server i:%d port:%d", i, port)
        //port := clusterPortStart + i
        server.Init(clusterIp, port, pwd, "m" + strconv.Itoa(i) + "_")
        cfgArgs := make(map[string]string)
        cfgArgs["maxBinlogKeepNum"] = "100"
        cfgArgs["kvstorecount"] = strconv.Itoa(kvstorecount)
        cfgArgs["cluster-enabled"] = "true"
        cfgArgs["pauseTimeIndexMgr"] = "1"
        cfgArgs["rocks.blockcachemb"] = "24"
        cfgArgs["requirepass"] = "tendis+test"
        cfgArgs["masterauth"] = "tendis+test"
        cfgArgs["generalLog"] = "true"
        cfgArgs["cluster-migration-slots-num-per-task"] = "10000"
        if err := server.Setup(false, &cfgArgs); err != nil {
            log.Fatalf("setup failed,port:%s err:%v", port, err)
        }
        servers = append(servers, server)
    }
    time.Sleep(15 * time.Second)

    // meet
    log.Infof("cluster meet begin")
    cli0 := createClient(&servers[0])
    for i := 1; i < clusterNodeNum * 2; i++ {
        if r, err := cli0.Cmd("cluster", "meet", servers[i].Ip, servers[i].Port).Str();
            r != ("OK") {
            log.Fatalf("meet failed:%v %s", err, r)
            return nil, nil, nil
        }
    }
    time.Sleep(2 * time.Second)

    // slaveof
    log.Infof("cluster slaveof begin")
    for i := clusterNodeNum; i < clusterNodeNum * 2; i++ {
        cli := createClient(&servers[i])
        masterIndex := i - clusterNodeNum
        masterNodeName := getNodeName(&servers[masterIndex])
        if r, err := cli.Cmd("cluster", "replicate", masterNodeName).Str();
            r != ("OK") {
            log.Fatalf("replicate failed:%v %s", err, r)
            return nil, nil, nil
        }
    }

    // add slot
    log.Infof("cluster addslots begin")
    for i := 0; i < clusterNodeNum; i++ {
        cli := createClient(&servers[i])
        slots := "{" + strconv.Itoa(nodeInfoArray[i].startSlot) + ".." + strconv.Itoa(nodeInfoArray[i].endSlot) + "}"
        log.Infof("addslot %d %s", i, slots)
        if r, err := cli.Cmd("cluster", "addslots", slots).Str();
            r != ("OK") {
            log.Fatalf("addslots failed:%v %s", err, r)
            return nil, nil, nil
        }
    }

    // start predixy
    //portPredixy := clusterPortStart + clusterNodeNum * 2 + 10
    portPredixy := util.FindAvailablePort(clusterPortStart)
    log.Infof("start server Predixy port:%d", portPredixy)
    predixy := util.Predixy{}
    ip := "127.0.0.1"
    predixy.Init(ip, portPredixy, ip, servers[0].Port, pwd, "predixy_")
    predixyCfgArgs := make(map[string]string)
    if err := predixy.Setup(false, &predixyCfgArgs); err != nil {
        log.Fatalf("setup failed:%v", err)
    }

    // TODO(takenliu) why need 15s for 5 nodes to change CLUSTER_OK ???
    time.Sleep(20 * time.Second)

    return &servers, &predixy, &nodeInfoArray
}

func stopCluster(servers *[]util.RedisServer, clusterNodeNum int, predixy *util.Predixy){
    for i := 0; i < clusterNodeNum*2; i++ {
        shutdownServer(&(*servers)[i], *shutdown, *clear);
    }
    shutdownPredixy(predixy, *shutdown, *clear)
}
