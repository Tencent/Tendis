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
    defer cli.Close()

    if r, err := cli.Cmd("cluster", "meet", s.Ip, s.Port).Str(); err != nil {
        log.Fatalf("do meet failed:%v", err)
        return
    } else if r != "OK" {
        log.Fatalf("do meet error:%s", r)
        return
    }
    log.Infof("meet sucess,mport:%d sport:%d" , m.Port, s.Port)
}

func cluster_set_node_arbiter(m *util.RedisServer) {
    cli := createClient(m)
    defer cli.Close()

    if r, err := cli.Cmd("cluster", "asarbiter").Str(); err != nil {
        log.Fatalf("cluster set arbiter failed:%v", err)
    } else if r != "OK" {
        log.Fatalf("cluster set arbiter error:%s", r)
    }

    log.Infof("cluster set arbiter[%s] ok", m.Addr())
}

func cluster_manual_failover(m *util.RedisServer) {
    cli := createClient(m)
    defer cli.Close()

    if r, err := cli.Cmd("cluster", "failover").Str(); err != nil {
        log.Fatalf("execute manual failover failed:%v", err)
    } else if r != "OK" {
        log.Fatalf("execute manual failover error:%s", r)
    }
}

func cluster_extract_field(reply string, prefix string, delimiter string) string {
    rest := strings.Split(reply, prefix)
    tmp := strings.Split(rest[1], delimiter)

    return tmp[0]
}

func cluster_check_state(m *util.RedisServer) bool {
    cli := createClient(m)
    defer cli.Close()

    reply, err := cli.Cmd("cluster", "info").Str()
    if err != nil {
        log.Fatalf("cluster info failed:%v", err)
    }

    state := cluster_extract_field(reply, "cluster_state:", "\r\n")
    log.Infof("check cluster state, state[%s]", state)

    return state == "ok"
}

func cluster_check_is_master(m *util.RedisServer) bool {
    cli := createClient(m)
    defer cli.Close()

    reply, err := cli.Cmd("info", "replication").Str()
    if err != nil {
        log.Fatalf("info replication failed:%v", err)
    }

    role := cluster_extract_field(reply, "role:", "\r\n")
    log.Infof("check node role, role[%s]", role)

    return role == "master"
}

func cluster_check_repl_offset(m *util.RedisServer, s *util.RedisServer) bool {
    scli := createClient(s)
    defer scli.Close()

    // first get slave_repl_offset, 
    // in order to avoid master ~= slave -> master == slave
    // because extract field time gap
    reply, err := scli.Cmd("info", "replication").Str()
    if err != nil {
        log.Fatalf("info replication failed:%v", err)
    }
    slave_repl_offset := cluster_extract_field(reply, "slave_repl_offset:", "\r\n")

    // wait for 1 second, in ordrer to avoid benchmark error casue offset increase late for a while
    time.Sleep(1 * time.Second)

    mcli := createClient(m)
    defer mcli.Close()

    reply, err = mcli.Cmd("info", "replication").Str()
    if err != nil {
        log.Fatalf("info replication failed:%v", err)
    }
    master_repl_offset := cluster_extract_field(reply, "master_repl_offset:", "\r\n")
    
    if master_repl_offset == slave_repl_offset {
        log.Infof("check master_repl_offset[%s] == slave_repl_offset[%s]", 
                master_repl_offset, slave_repl_offset)
        return true
    } else {
        log.Infof("check master_repl_offset[%s] != slave_repl_offset[%s]", 
                master_repl_offset, slave_repl_offset)
        return false
    }
}

func cluster_check_sync_full(m *util.RedisServer, times int) bool {
    cli := createClient(m)
    defer cli.Close()

    reply, err := cli.Cmd("info", "stats").Str()
    if err != nil {
        log.Fatalf("info stats failed:%v", err)
    }

    data := cluster_extract_field(reply, "sync_full:", "\r\n")
    sync_full, _ := strconv.Atoi(data)
    log.Infof("check node[%s] sync_full[%d] <==> times[%d]", 
            m.Addr(), sync_full, times)
    if sync_full == times {
        return true;
    } else {
        return false;
    }
}

func cluster_check_sync_partial_ok(m *util.RedisServer, times int) bool {
    cli := createClient(m)
    defer cli.Close()

    reply, err := cli.Cmd("info", "stats").Str()
    if err != nil {
        log.Fatalf("info stats failed:%v", err)
    }

    data := cluster_extract_field(reply, "sync_partial_ok:", "\r\n")
    sync_partial_ok, _ := strconv.Atoi(data)
    log.Infof("check node[%s] sync_partial_ok[%d] <==> times[%d]", 
            m.Addr(), sync_partial_ok, times)
    if sync_partial_ok == times {
        return true;
    } else {
        return false;
    }
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

func startCluster(
    clusterIp string, clusterPortStart int,
    clusterNodeNum int, externalArgs map[string]string, pwd string, binpath string,
) (*[]util.RedisServer, *util.Predixy, *[]NodeInfo) {
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
        if binpath != "" {
            server.WithBinPath(binpath)
        }
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
        for k, v := range externalArgs {
            cfgArgs[k] = v
        }
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

func checkFullsyncSuccTimes(m *util.RedisServer, num int) {
    cli := createClient(m)
    r, err := cli.Cmd("info", "replication").Str()
    if err != nil {
        log.Fatalf("cluster countkeysinslot failed:%v %s", err, r)
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

func getClusterNodes(m *util.RedisServer) string {
    cli := createClient(m)
    r, err := cli.Cmd("cluster", "nodes").Str()
    if err != nil {
        log.Fatalf("cluster countkeysinslot failed:%v %s", err, r)
    }
    return r
}

func isMaster(m *util.RedisServer) bool {
    expectMaster := "myself,master"
    nodeString := getClusterNodes(m)
    return strings.Contains(nodeString, expectMaster)
}