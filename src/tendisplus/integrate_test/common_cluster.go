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