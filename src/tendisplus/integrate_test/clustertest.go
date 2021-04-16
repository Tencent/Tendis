// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

package main

import (
    "flag"
    "github.com/ngaut/log"
    "tendisplus/integrate_test/util"
    "strconv"
    "time"
    "os/exec"
    "fmt"
    "strings"
    "github.com/mediocregopher/radix.v2/redis"
)

func printNodes(m *util.RedisServer) {
    cli := createClient(m)
    output, err := cli.Cmd("cluster", "nodes").Str();
    if err != nil {
        fmt.Print(err)
        return
    }
    log.Infof("printNodes sucess. %s:%d nodes:%s", m.Ip, m.Port, output)
}

func addDataByClientInCoroutine(m *util.RedisServer, num int, prefixkey string, channel chan int) {
    var optype string = *benchtype
    //log.Infof("optype is : %s", optype)
    switch optype {
        case "set" : 
            log.Infof("optype is : %s", optype)
            addDataByClient(m, num, prefixkey)
        case "sadd" : 
            log.Infof("optype is : %s", optype)
            addSetDataByClient(m, num, prefixkey)
        case "hmset" :
            log.Infof("optype is : %s", optype)
            addHashDataByClient(m, num, prefixkey)
        case "rpush" :
            log.Infof("optype is : %s", optype)
            addListDataByClient(m, num, prefixkey)
        case "zadd" :
            log.Infof("optype is : %s", optype)
            addSortedDataByClient(m, num, prefixkey)
        default : 
            log.Infof("no benchtype")
	}
    //addDataByClient(m, num, prefixkey)
    channel <- 0
}

// set key value
func addDataByClient(m *util.RedisServer, num int, prefixkey string) {
    log.Infof("addData begin. %s:%d", m.Ip, m.Port)
    // TODO(takenliu):mset dont support moved ???
    for i := 0; i < num; i++ {
        var args []string
        args = append(args,  "-h", m.Ip, "-p", strconv.Itoa(m.Port),
            "-c", "-a", *auth, "set")

        key := "key" + prefixkey + "_" + strconv.Itoa(i)
        value := "value" + prefixkey + "_" + strconv.Itoa(i)
        args = append(args, key)
        args = append(args, value)

        cmd := exec.Command("../../../bin/redis-cli", args...)
        //cmd := exec.Command("../../../bin/redis-cli", "-h", m.Ip, "-p", strconv.Itoa(m.Port),
        //    "-c", "-a", *auth, "mset", kvs...)
        data, err := cmd.Output()
        //fmt.Print(string(output))
        if string(data) != "OK\n" || err != nil {
            log.Infof("set failed, key:%v data:%s err:%v", key, data, err)
        }
    }
    log.Infof("addData sucess. %s:%d num:%d", m.Ip, m.Port, num)
}

// sadd key elements [elements...]
func addSetDataByClient(m *util.RedisServer, num int, prefixkey string) {
    log.Infof("addData begin(Sadd). %s:%d", m.Ip, m.Port)

    for i := 0; i < num; i++ {
        var args []string
        args = append(args, "-h", m.Ip, "-p", strconv.Itoa(m.Port),
            "-c", "-a", *auth, "sadd")

        key := "key" + prefixkey + "_" + strconv.Itoa(i)
        args = append(args, key)
        for h := 0; h < 2; h++ {
            value := "value" + prefixkey + "_" + strconv.Itoa(h)
            args = append(args, value)
		}
        
        cmd := exec.Command("../../../bin/redis-cli", args...)
        data, err := cmd.Output()
        //log.Infof("sadd:  %s", data)
        if string(data) != "2\n" || err != nil {
            log.Infof("sadd failed, key%v data:%s err:%v", key, data, err)  
		}
	}
    log.Infof("addData success(Sadd). %s:%d num:%d", m.Ip, m.Port, num)
}

func addHashDataByClient(m *util.RedisServer, num int, prefixkey string) {
    log.Infof("addData begin(Hmset). %s:%d", m.Ip, m.Port)

    for i := 0; i < num; i++ {
        var args []string
        args = append(args, "-h", m.Ip, "-p", strconv.Itoa(m.Port),
            "-c", "-a", *auth, "hmset")

        key := "key" + prefixkey + "_" + strconv.Itoa(i)
        args = append(args, key)
        for h := 0; h < 2; h++ {
            value := "value" + prefixkey + "_" + strconv.Itoa(h)
            args = append(args, strconv.Itoa(h))
            args = append(args, value)
		}
        
        cmd := exec.Command("../../../bin/redis-cli", args...)
        data, err := cmd.Output()
        //log.Infof("sadd:  %s", data)
        if string(data) != "OK\n" || err != nil {
            log.Infof("hmset failed, key%v data:%s err:%v", key, data, err)  
		}
	}
    log.Infof("addData success(Hmset). %s:%d num:%d", m.Ip, m.Port, num)
}

func addListDataByClient(m *util.RedisServer, num int, prefixkey string) {
    log.Infof("addData begin(Rpush). %s:%d", m.Ip, m.Port)

    for i := 0; i < num; i++ {
        var args []string
        args = append(args, "-h", m.Ip, "-p", strconv.Itoa(m.Port),
            "-c", "-a", *auth, "rpush")

        key := "key" + prefixkey + "_" + strconv.Itoa(i)
        args = append(args, key)
        for h := 0; h < 2; h++ {
            value := "value" + prefixkey + "_" + strconv.Itoa(h)
            args = append(args, value)
		}
        
        cmd := exec.Command("../../../bin/redis-cli", args...)
        data, err := cmd.Output()
        //log.Infof("sadd:  %s", data)
        if string(data) != "2\n" || err != nil {
            log.Infof("Rpush failed, key%v data:%s err:%v", key, data, err)  
		}
	}
    log.Infof("addData success(Rpush). %s:%d num:%d", m.Ip, m.Port, num)
}

func addSortedDataByClient(m *util.RedisServer, num int, prefixkey string) {
    log.Infof("addData begin(Zadd). %s:%d", m.Ip, m.Port)

    for i := 0; i < num; i++ {
        var args []string
        args = append(args, "-h", m.Ip, "-p", strconv.Itoa(m.Port),
            "-c", "-a", *auth, "zadd")

        key := "key" + prefixkey + "_" + strconv.Itoa(i)
        args = append(args, key)
        for h := 0; h < 1; h++ {
            value := "value" + prefixkey + "_" + strconv.Itoa(h)
            args = append(args, strconv.Itoa(h))
            args = append(args, value)
		}
        
        cmd := exec.Command("../../../bin/redis-cli", args...)
        data, err := cmd.Output()
        //log.Infof("sadd:  %s", data)
        if string(data) != "1\n" || err != nil {
            log.Infof("Zadd failed, key%v data:%s err:%v", key, data, err)  
		}
	}
    log.Infof("addData success(Zadd). %s:%d num:%d", m.Ip, m.Port, num)
}

func checkDataInCoroutine2(m *[]util.RedisServer, num int, prefixkey string, channel chan int) {
    var optype string = *benchtype
    //log.Infof("optype is : %s", optype)
    switch optype {
        case "set" : 
            log.Infof("optype is : %s", optype)
            checkData2(m, num, prefixkey)
        case "sadd" : 
            log.Infof("optype is : %s", optype)
            checkSetData2(m, num, prefixkey)
        case "hmset" : 
            log.Infof("optype is : %s", optype)
            checkHashData2(m, num, prefixkey)
        case "rpush" : 
            log.Infof("optype is : %s", optype)
            checkListData2(m, num, prefixkey)
        case "zadd" : 
            log.Infof("optype is : %s", optype)
            checkSortedData2(m, num, prefixkey)
        default : 
            log.Infof("no benchtype:%s", optype)
	}
    //checkData2(m, num, prefixkey)
    channel <- 0
}

func checkData2(m *[]util.RedisServer, num int, prefixkey string) {
    log.Infof("checkData begin. prefixkey:%s", prefixkey)

    cli := createClient(&(*m)[0])
    for i := 0; i < num; i++ {
        // redis-cli will process moved station in get command
        key := "key" + prefixkey + "_" + strconv.Itoa(i)
        value := "value" + prefixkey + "_" + strconv.Itoa(i)
        data, err := cli.Cmd("get", key).Str();
        if err != nil {
            log.Infof("get failed, key:%v data:%s err:%v", key, data, err)
        }

        retValue := strings.Replace(string(data), "\n", "", -1)
        if retValue != value {
            log.Infof("find failed, key:%v data:%s value:%s err:%v", key, retValue, value, err)
        }
    }
    log.Infof("checkData end. prefixkey:%s", prefixkey)
}

func checkSetData2(m *[]util.RedisServer, num int, prefixkey string) {
    log.Infof("checkData begin(Sadd). prefixkey:%s", prefixkey)

    for i := 0; i < num; i++ {
        var args []string
        args = append(args,  "-h", (*m)[0].Ip, "-p", strconv.Itoa((*m)[0].Port),
            "-c", "-a", *auth, "scard")

        key := "key" + prefixkey + "_" + strconv.Itoa(i)
        value := "2"
        args = append(args, key)

        cmd := exec.Command("../../../bin/redis-cli", args...)
        data, err := cmd.Output()

        retValue := strings.Replace(string(data), "\n", "", -1)
        if retValue != value {
            log.Infof("find failed(command : scard), key:%v data:%s value:%s err:%v", key, retValue, value, err)
        }
	}
    log.Infof("checkData end(Sadd). prefixkey:%s", prefixkey)
}

func checkHashData2(m *[]util.RedisServer, num int, prefixkey string) {
    log.Infof("checkData begin(Hmset). prefixkey:%s", prefixkey)

    for i := 0; i < num; i++ {
        var args []string
        args = append(args,  "-h", (*m)[0].Ip, "-p", strconv.Itoa((*m)[0].Port),
            "-c", "-a", *auth, "hlen")

        key := "key" + prefixkey + "_" + strconv.Itoa(i)
        value := "2"
        args = append(args, key)

        cmd := exec.Command("../../../bin/redis-cli", args...)
        data, err := cmd.Output()

        retValue := strings.Replace(string(data), "\n", "", -1)
        if retValue != value {
            log.Infof("find failed(command : hlen), key:%v data:%s value:%s err:%v", key, retValue, value, err)
        }
	}
    log.Infof("checkData end(Hmset). prefixkey:%s", prefixkey)
}

func checkListData2(m *[]util.RedisServer, num int, prefixkey string) {
    log.Infof("checkData begin(Rpush). prefixkey:%s", prefixkey)

    for i := 0; i < num; i++ {
        var args []string
        args = append(args,  "-h", (*m)[0].Ip, "-p", strconv.Itoa((*m)[0].Port),
            "-c", "-a", *auth, "llen")

        key := "key" + prefixkey + "_" + strconv.Itoa(i)
        value := "2"
        args = append(args, key)

        cmd := exec.Command("../../../bin/redis-cli", args...)
        data, err := cmd.Output()

        retValue := strings.Replace(string(data), "\n", "", -1)
        if retValue != value {
            log.Infof("find failed(command : llen), key:%v data:%s value:%s err:%v", key, retValue, value, err)
        }
	}
    log.Infof("checkData end(Rpush). prefixkey:%s", prefixkey)
}

func checkSortedData2(m *[]util.RedisServer, num int, prefixkey string) {
    log.Infof("checkData begin(Zadd). prefixkey:%s", prefixkey)

    for i := 0; i < num; i++ {
        var args []string
        args = append(args,  "-h", (*m)[0].Ip, "-p", strconv.Itoa((*m)[0].Port),
            "-c", "-a", *auth, "zcard")

        key := "key" + prefixkey + "_" + strconv.Itoa(i)
        value := "1"
        args = append(args, key)

        cmd := exec.Command("../../../bin/redis-cli", args...)
        data, err := cmd.Output()

        retValue := strings.Replace(string(data), "\n", "", -1)
        if retValue != value {
            log.Infof("find failed(command : zcard), key:%v data:%s value:%s err:%v", key, retValue, value, err)
        }
	}
    log.Infof("checkData end(Zadd). prefixkey:%s", prefixkey)
}

func checkSlots(servers *[]util.RedisServer, serverIdx int, nodeInfoArray *[]NodeInfo,
    clusterNodeNum int, dstNodeIndex int, checkself bool) {
    log.Infof("checkSlots begin idx:%d checkself:%v", serverIdx, checkself)

    cli0 := createClient(&(*servers)[serverIdx])
    ret := cli0.Cmd("cluster", "slots")
    log.Infof("checkSlotsInfo0 :%s", ret)
    if (!ret.IsType(redis.Array)) {
        log.Fatalf("cluster slots failed:%v", ret)
    }
    ret_array, _ := ret.Array()
    if !checkself && len(ret_array) != clusterNodeNum*2 {
        log.Fatalf("cluster slots size not right:%v", ret_array)
    }
    // example:
    // 1) 1) (integer) 1
    //    2) (integer) 16383
    //    3) 1) "127.0.0.1"
    //       2) (integer) 21002
    //       3) "0dd8a458cf74fbe16bbcbb5842143074c5fe5f5f"
    //    4) 1) "127.0.0.1"
    //       2) (integer) 21003
    //       3) "5f2b6f8f689253ca0a27d20c3175ec59565d8cfb"
    // 2) 1) (integer) 0
    //    2) (integer) 0
    //    3) 1) "127.0.0.1"
    //       2) (integer) 21000
    //       3) "ccb1f3183cdd0a91e6ef127417edc582ff7f0f78"
    //    4) 1) "127.0.0.1"
    //       2) (integer) 21001
    //       3) "29a66186a8d61836e870f6ddb9e6280b22321348"
    for _,value := range ret_array {
        // log.Infof("checkSlotsInfo1 :%s", value)
        if !value.IsType(redis.Array) {
            log.Fatalf("cluster slots data not array:%v", value)
        }
        ret_array2, _ := value.Array()
        if len(ret_array2) != 4 ||
            !ret_array2[0].IsType(redis.Int) ||
            !ret_array2[1].IsType(redis.Int) ||
            !ret_array2[2].IsType(redis.Array) ||
            !ret_array2[3].IsType(redis.Array) {
            log.Fatalf("cluster slots value not right:%v", ret_array2)
        }
        startSlot,_ := ret_array2[0].Int()
        endSlot,_ := ret_array2[1].Int()
        ret_array_master, _ := ret_array2[2].Array()
        ret_array_slave, _ := ret_array2[3].Array()
        if len(ret_array_master) != 3 ||
            !ret_array_master[0].IsType(redis.Str) ||
            !ret_array_master[1].IsType(redis.Int) ||
            !ret_array_master[0].IsType(redis.Str){
            log.Fatalf("cluster slots value not right:%v", ret_array_master)
        }
        if len(ret_array_slave) != 3 ||
            !ret_array_slave[0].IsType(redis.Str) ||
            !ret_array_slave[1].IsType(redis.Int) ||
            !ret_array_slave[0].IsType(redis.Str){
            log.Fatalf("cluster slots value not right:%v", ret_array_slave)
        }
        ip,_ := ret_array_master[0].Str()
        port,_ := ret_array_master[1].Int()
        nodeName,_ := ret_array_master[2].Str()
        port_slave,_ := ret_array_slave[1].Int()
        if checkself && port != (*servers)[serverIdx].Port && port_slave != (*servers)[serverIdx].Port {
            continue
        }
        nodeIndex := -1
        for i := 0; i < len(*servers); i++ {
            if ((*servers)[i].Port == port) {
                nodeIndex = i;
            }
        }
        if nodeIndex == -1 {
            log.Fatalf("startslot:%v endslot:%v ip:%v port:%v port_slave:%v nodename:%v nodeIndex:%v",
                startSlot, endSlot, ip, port, port_slave, nodeName, nodeIndex)
        }
        log.Infof("startslot:%v endslot:%v ip:%v port:%v port_slave:%v nodename:%v nodeIndex:%v",
            startSlot, endSlot, ip, port, port_slave, nodeName, nodeIndex)
        // check src nodes
        if nodeIndex < clusterNodeNum &&
            (startSlot != (*nodeInfoArray)[nodeIndex].startSlot ||
            endSlot != (*nodeInfoArray)[nodeIndex].migrateStartSlot - 1) {
            log.Fatalf("cluster slots not right,startSlot:%v endSlot:%v cluster slots:%v",
                (*nodeInfoArray)[nodeIndex].startSlot, (*nodeInfoArray)[nodeIndex].migrateStartSlot-1,
                ret_array2)
        }
        // check slave port
        if (nodeIndex != dstNodeIndex && (*servers)[nodeIndex + clusterNodeNum].Port != port_slave) ||
            (nodeIndex == dstNodeIndex && (*servers)[nodeIndex + 1].Port != port_slave) {
            log.Fatalf("cluster slots not right,master port:%v slave port:%v ret_array2:%v",
                port, port_slave, ret_array2)
        }

        // check dst node
        if nodeIndex == dstNodeIndex {
            find := false
            for _, nodeInfo := range *nodeInfoArray {
                if startSlot == nodeInfo.migrateStartSlot && endSlot == nodeInfo.migrateEndSlot {
                    find = true
                }
            }
            if !find {
                log.Fatalf("cluster slots not right,master port:%v slave port:%v ret_array2:%v",
                    port, port_slave, ret_array2)
            }
        }
    }
    log.Infof("checkSlots end idx:%d checkself:%v", serverIdx, checkself)
}
func testCluster(clusterIp string, clusterPortStart int, clusterNodeNum int) {
    var nodeInfoArray []NodeInfo
    perNodeMigrateNum := CLUSTER_SLOTS / (clusterNodeNum+1) /clusterNodeNum
    // NOTE(takenliu) if only on node, migrate CLUSTER_SLOTS-1 slots to dst node.
    if clusterNodeNum == 1 {
        perNodeMigrateNum = CLUSTER_SLOTS - 1
    }
    for i := 0; i <= clusterNodeNum; i++ {
        var startSlot = CLUSTER_SLOTS / clusterNodeNum * i;
        var endSlot = startSlot + CLUSTER_SLOTS / clusterNodeNum - 1;
        if i == (clusterNodeNum - 1) {
            endSlot = CLUSTER_SLOTS - 1;
        }
        var migrateStart = endSlot - perNodeMigrateNum + 1
        if migrateStart <= startSlot{
            migrateStart = startSlot
        }
        migrateEnd := endSlot
        nodeInfoArray = append(nodeInfoArray,
            NodeInfo{i, startSlot, endSlot, migrateStart, migrateEnd})
    }

    pwd := getCurrentDirectory()
    log.Infof("current pwd:" + pwd)
    kvstorecount := 2

    // migrate from node[0, clusterNodeNum-1] to node[clusterNodeNum*2-1]
    // master:[0, clusterNodeNum-1] slave:[clusterNodeNum, clusterNodeNum*2-1]
    // dst master:[clusterNodeNum*2], dst slave [clusterNodeNum*2 + 1]
    dstNodeIndex := clusterNodeNum * 2
    var servers []util.RedisServer

    // start server
    log.Infof("start servers clusterNodeNum:%d", clusterNodeNum)
    for i := 0; i <= clusterNodeNum * 2 + 1; i++ {
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
        cfgArgs["migrate-gc-enabled"] = "true"
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
            return
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
            return
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
            return
        }
    }

    // meet the dst node
    log.Infof("cluster meet the dst node")
    for i := dstNodeIndex; i <= dstNodeIndex+1; i++ {
        if r, err := cli0.Cmd("cluster", "meet", servers[i].Ip, servers[i].Port).Str();
            r != ("OK") {
            log.Fatalf("meet failed:%v %s", err, r)
            return
        }
    }
    time.Sleep(2 * time.Second)

    // dst node slaveof
    {
        cli := createClient(&servers[dstNodeIndex + 1])
        masterNodeName := getNodeName(&servers[dstNodeIndex])
        if r, err := cli.Cmd("cluster", "replicate", masterNodeName).Str();
            r != ("OK") {
            log.Fatalf("replicate failed:%v %s", err, r)
            return
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

    // add data
    log.Infof("cluster add data begin")
    var channel chan int = make(chan int)
    for i := 0; i < clusterNodeNum; i++ {
        go addDataInCoroutine(&predixy.RedisServer, *num1, strconv.Itoa(i), channel)
    }

    // if add 10000 key, need about 15s
    time.Sleep(20 * time.Second)

    // migrate
    log.Infof("cluster migrate begin")
    cliDst := createClient(&servers[dstNodeIndex])
    dstNodeName := getNodeName(&servers[dstNodeIndex])
    log.Infof("cluster migrate dstNodeName:%s perNodeMigrateNum:%d", dstNodeName, perNodeMigrateNum)
    for i := 0; i < clusterNodeNum; i++ {
        printNodes(&servers[i])
        srcNodeName := getNodeName(&servers[i])
        //cli := createClient(&servers[i])

        var slots []int
        for j := nodeInfoArray[i].migrateStartSlot; j <= nodeInfoArray[i].migrateEndSlot; j++ {
            slots = append(slots, j)
        }
        log.Infof("migrate node:%d srcNodename:%s slots:%v", i, srcNodeName, slots)
        /*if r, err := cli.Cmd("cluster", "setslot", "migrating", dstNodeName, slots).Str();
            r != ("OK") {
            log.Fatalf("migrating failed:%v %s", err, r)
            return
        }
        time.Sleep(1 * time.Second)*/
        
        if r, err := cliDst.Cmd("cluster", "setslot", "importing", srcNodeName, slots).Str();
            err != nil {
            log.Fatalf("importing failed:%v %s", err, r)
            return
        }
    }

    // wait addDataInCoroutine
    for i := 0; i < clusterNodeNum; i++ {
        <- channel
    }
    log.Infof("cluster add data end")

    // NOTE(takenliu): if migrateTaskSlotsLimit is smaller, need wait longer time for migrate.
    time.Sleep(50 * time.Second)
    // gossip hasn't sync info sucess, so check self slots.
    // master will send binlog to slave, so slave will change slots info immediately
    checkself := true
    // master
    checkSlots(&servers, 0, &nodeInfoArray, clusterNodeNum, dstNodeIndex, checkself)
    // slave
    checkSlots(&servers, clusterNodeNum, &nodeInfoArray, clusterNodeNum, dstNodeIndex, checkself)
    // dst node master
    checkSlots(&servers, dstNodeIndex, &nodeInfoArray, clusterNodeNum, dstNodeIndex, checkself)
    // dst node slave
    checkSlots(&servers, dstNodeIndex + 1, &nodeInfoArray, clusterNodeNum, dstNodeIndex, checkself)

    // wait gossip sync info, and check all nodes
    time.Sleep(30 * time.Second)
    checkself = false
    // master
    checkSlots(&servers, 0, &nodeInfoArray, clusterNodeNum, dstNodeIndex, checkself)
    // slave
    checkSlots(&servers, clusterNodeNum, &nodeInfoArray, clusterNodeNum, dstNodeIndex, checkself)
    // dst node master
    checkSlots(&servers, dstNodeIndex, &nodeInfoArray, clusterNodeNum, dstNodeIndex, checkself)
    // dst node slave
    checkSlots(&servers, dstNodeIndex + 1, &nodeInfoArray, clusterNodeNum, dstNodeIndex, checkself)
	
     // check keys num
    masterTotalKeyNum := 0
    slaveTotalKeyNum := 0
    var nodesKeyNum []int
    for i := 0; i <= clusterNodeNum*2 + 1; i++ {
        beslave := false
        masterIndex := i
        if (i >= clusterNodeNum && i < clusterNodeNum*2) {
            beslave = true
            masterIndex = i - clusterNodeNum
        }
        if i == dstNodeIndex + 1 {
            beslave = true
            masterIndex = dstNodeIndex
        }
        cli := createClient(&servers[i])

        nodeKeyNum := 0
        for j := 0; j < CLUSTER_SLOTS; j++ {
            r, err := cli.Cmd("cluster", "countkeysinslot", j).Int();
            if err != nil {
                log.Fatalf("cluster countkeysinslot failed:%v %s", err, r)
                return
            }
            if r != 0 {
                // log.Infof("cluster countkeysinslot, server:%d slot:%d num:%d", i, j, r)
            }
            nodeKeyNum += r
            // check src node migrated slot key num should be 0
            if i < clusterNodeNum*2 && j < nodeInfoArray[masterIndex].startSlot && r != 0 {
                log.Fatalf("check keys num failed,server:%v slot:%v keynum:%v",
                    i, j, r)
            }
            if i < clusterNodeNum*2 && j >= nodeInfoArray[masterIndex].migrateStartSlot && r != 0 {
                log.Fatalf("check keys num failed,server:%v slot:%v keynum:%v",
                    i, j, r)
            }
            // TODO(takenliu): check dst node
        }
        log.Infof("check keys num server:%d keynum:%d beslave:%v", i, nodeKeyNum, beslave)
        if beslave {
            slaveTotalKeyNum += nodeKeyNum

            if nodeKeyNum != nodesKeyNum[masterIndex] {
                log.Fatalf("check keys num failed,server:%v selfKeyNum:%v masterKeyNum:%v",
                                i, nodeKeyNum, nodesKeyNum[masterIndex])
            }
        } else {
            masterTotalKeyNum += nodeKeyNum
        }
        nodesKeyNum = append(nodesKeyNum, nodeKeyNum)
    }
    log.Infof("check keys num masterTotalKeyNum:%d slaveTotalKeyNum:%d", masterTotalKeyNum, slaveTotalKeyNum)
    if masterTotalKeyNum != clusterNodeNum * *num1 {
        var fake_servers []util.RedisServer
        fake_servers = append(fake_servers, predixy.RedisServer)
        for i := 0; i < clusterNodeNum; i++ {
            go checkDataInCoroutine2(&fake_servers, *num1, strconv.Itoa(i), channel)
        }
        for i := 0; i < clusterNodeNum; i++ {
            <- channel
        }
        log.Fatalf("check keys num failed:%d != %d", masterTotalKeyNum, clusterNodeNum * *num1)
    }
    if slaveTotalKeyNum != clusterNodeNum * *num1 {
        log.Fatalf("check keys num failed:%d != %d", slaveTotalKeyNum, clusterNodeNum * *num1)
    }

    for i := 0; i <= clusterNodeNum*2 + 1; i++ {
        shutdownServer(&servers[i], *shutdown, *clear);
    }
    shutdownPredixy(&predixy, *shutdown, *clear)
}

func main(){
    flag.Parse()
    // rand.Seed(time.Now().UTC().UnixNano())
    testCluster(*clusterIp, *clusterPortStart, 1)
    testCluster(*clusterIp, *clusterPortStart + 100, *clusterNodeNum)
    log.Infof("clustertest.go passed. command : %s", *benchtype)
}
