package main

import (
    "flag"
    "github.com/ngaut/log"
    "tendisplus/integrate_test/util"
    "strconv"
    "time"
    "math"
)

func checkSlotKeyNum(servers *util.RedisServer, slot int, expKeynum int) {
    cli := createClient(servers)
    r, err := cli.Cmd("cluster", "countkeysinslot", slot).Int();
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
    r, err := cli.Cmd("cluster", "countkeysinslot", slot).Int();
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

func checkDbsize(servers *util.RedisServer, expKeynum int) {
    cli := createClient(servers)
    r, err := cli.Cmd("dbsize").Int();
    if err != nil {
        log.Fatalf("cluster countkeysinslot failed:%v %s", err, r)
        return
    }
    if r != expKeynum {
        log.Fatalf("checkDbsize failed, server:%d num:%d expKeynum:%d",
            servers.Port, r, expKeynum)
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
    fake_redis := util.RedisServer{}
    fake_redis.Init(predixy.Ip, predixy.Port, "", "")
    go addDataInCoroutine(&fake_redis, num, "{12}", channel)

    log.Infof("cluster backup begin")
    time.Sleep(1 * time.Second)
    backup(src_slave, backup_mode, "/tmp/back_srcmaster")
    backup(dst_slave, backup_mode, "/tmp/back_dstmaster")

    // migrate
    log.Infof("cluster migrate begin")
    cluster_migrate(src_master, dst_master, migSlot, migSlot)
    var tsBeforeMigrateEnd uint64 = (uint64)(time.Now().UnixNano()) / 1000000

    // wait add data end
    <- channel
    log.Infof("cluster adddata end")


    // meet new cluster
    log.Infof("cluster meet begin")
    cluster_meet(src_restore, dst_restore)
    time.Sleep(2 * time.Second)

    // addslots new cluster
    log.Infof("cluster addslots begin")
    cluster_addslots(src_restore, 0, 10000)
    cluster_addslots(dst_restore, 10001, 16383)
    time.Sleep(10 * time.Second)


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


    log.Infof("checkData begin")
    checkSlotEmpty(src_restore, migSlot, false)
    checkSlotEmpty(dst_restore, migSlot, true)

    checkDbsize(src_master, 0)
    checkDbsize(src_slave, 0)
    checkDbsize(dst_master, num)
    checkDbsize(dst_slave, num)
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
    fake_redis := util.RedisServer{}
    fake_redis.Init(predixy.Ip, predixy.Port, "", "")
    go addDataInCoroutine(&fake_redis, num, "{12}", channel)

    // migrate
    log.Infof("cluster migrate begin")
    cluster_migrate(src_master, dst_master, migSlot, migSlot)

    time.Sleep(1 * time.Second)
    log.Infof("cluster backup begin")
    backup(src_slave, backup_mode, "/tmp/back_srcmaster")
    backup(dst_slave, backup_mode, "/tmp/back_dstmaster")

    // wait add data end
    <- channel
    log.Infof("cluster adddata end")

    //waitDumpBinlog(src_slave, kvstorecount)
    //waitDumpBinlog(dst_slave, kvstorecount)
    time.Sleep(5 * time.Second)
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
    // TODO(takenliu) add gotest for the two diffrent case ???
    restoreBinlog(src_slave, src_restore, kvstorecount, tsDeleteChunkNotFinish)
    restoreBinlogEnd(src_restore, kvstorecount)

    log.Infof("restoreBackup begin")
    restoreBackup(dst_restore, "/tmp/back_dstmaster")
    log.Infof("restoreBinlog begin")
    restoreBinlog(dst_slave, dst_restore, kvstorecount, tsDeleteChunkNotFinish)
    restoreBinlogEnd(dst_restore, kvstorecount)


    time.Sleep(3 * time.Second)

    log.Infof("checkData begin")
    checkSlotEmpty(src_restore, migSlot, true)
    checkSlotEmpty(dst_restore, migSlot, false)

    checkDbsize(src_master, 0)
    checkDbsize(src_slave, 0)
    checkDbsize(dst_master, num)
    checkDbsize(dst_slave, num)
    checkDbsize(src_restore, 0)
    // truncateBinlogV2 at least keep one binlog
    checkDbsize(dst_restore, num-1)
}

func testRestore(portStart int, num int, testFun int) {
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

    src_master.Init(ip, portStart, pwd, "src_master_")
    src_slave.Init(ip, portStart+1, pwd, "src_slave_")
    dst_master.Init(ip, portStart+2, pwd, "dst_master_")
    dst_slave.Init(ip, portStart+3, pwd, "dst_slave_")

    src_restore.Init(ip, portStart+4, pwd, "src_restore_")
    dst_restore.Init(ip, portStart+5, pwd, "dst_restore_")

    cfgArgs := make(map[string]string)
    cfgArgs["maxBinlogKeepNum"] = "1"
    cfgArgs["kvstorecount"] = strconv.Itoa(kvstorecount)
    cfgArgs["cluster-enabled"] = "true"
    cfgArgs["pauseTimeIndexMgr"] = "1"
    cfgArgs["rocks.blockcachemb"] = "24"
    cfgArgs["requirepass"] = "tendis+test"
    cfgArgs["masterauth"] = "tendis+test"
    cfgArgs["generalLog"] = "true"

    if err := src_master.Setup(*valgrind, &cfgArgs); err != nil {
        log.Fatalf("setup failed:%v", err)
    }
    if err := src_slave.Setup(*valgrind, &cfgArgs); err != nil {
        log.Fatalf("setup failed:%v", err)
    }
    if err := dst_master.Setup(*valgrind, &cfgArgs); err != nil {
        log.Fatalf("setup failed:%v", err)
    }
    if err := dst_slave.Setup(*valgrind, &cfgArgs); err != nil {
        log.Fatalf("setup failed:%v", err)
    }
    if err := src_restore.Setup(*valgrind, &cfgArgs); err != nil {
        log.Fatalf("setup failed:%v", err)
    }
    if err := dst_restore.Setup(*valgrind, &cfgArgs); err != nil {
        log.Fatalf("setup failed:%v", err)
    }

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
    portPredixy := portStart+6
    predixy := util.Predixy{}
    predixy.Init(ip, portPredixy, ip, portStart, pwd, "predixy_")
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

func main(){
    flag.Parse()
    testRestore(53000, 100000, 1)
    testRestore(53100, 100000, 2)
    log.Infof("clustertestRestore sucess")
}
    