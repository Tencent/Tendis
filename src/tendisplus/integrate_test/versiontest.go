// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"tendisplus/integrate_test/util"
	"time"

	"github.com/ngaut/log"
)

func testVersion(versions []string) {
	for _, v := range versions {
		// recreate test dir
		os.RemoveAll("_version_test_")
		os.Mkdir("_version_test_", 0750)
		// copy target version tendisplus
		r := exec.Command("/bin/sh", "-c", "cp /data/binary/tendisplus-"+v+".tgz _version_test_/; cd _version_test_/; tar -zxvf tendisplus-"+v+".tgz;")
		_, err := r.CombinedOutput()
		if err != nil {
			log.Fatalf("get target version tendisplus failed! check if /data/binary exists.")
		}

		clusterNodeNum := 3
		servers, predixy, _ := startCluster("127.0.0.1", 30000, clusterNodeNum, map[string]string{"timeoutSecBinlogWaitRsp": "3", "minBinlogKeepSec": "3600", "keysDefaultLimit": "10000000"}, util.GetCurrentDirectory()+"/_version_test_", "_version_test_/tendisplus-"+v+"/bin/tendisplus")

		// add data
		log.Infof("cluster add data begin")
		channel := make(chan int)
		for i := 0; i < 10; i++ {
			util.AddData(&predixy.RedisServer, int(rand.Int31n(20))+100, 0, strconv.Itoa(i), channel)
		}
		util.AddDataWithTime(&predixy.RedisServer, 60, 0, "longterm", channel)

		time.Sleep(1 * time.Second)

		for i := 0; i < clusterNodeNum; i++ {
			if !isMaster(&(*servers)[i]) {
				log.Fatalf("node should be master")
			}
			if !cluster_check_state(&(*servers)[i]) {
				log.Fatal("cluster state error, online failed")
			}
		}

		for i := clusterNodeNum; i < clusterNodeNum*2; i++ {
			if isMaster(&(*servers)[i]) {
				log.Fatalf("node should be slave")
			}
			if !cluster_check_state(&(*servers)[i]) {
				log.Fatal("cluster state error, online failed")
			}
		}

		totalKeyNumber := 0
		for i := 0; i < 10; i++ {
			totalKeyNumber += <-channel
		}

		// shutdown slaves
		for i := clusterNodeNum; i < clusterNodeNum*2; i++ {
			shutdownServer(&(*servers)[i], *shutdown, *clear)
		}

		time.Sleep(10 * time.Second)

		for i := clusterNodeNum; i < clusterNodeNum*2; i++ {
			(*servers)[i].WithBinPath("")
			(*servers)[i].Start(false, fmt.Sprintf("%s/test.cfg", (*servers)[i].Path))
		}

		time.Sleep(5 * time.Second)

		for i := clusterNodeNum; i < clusterNodeNum*2; i++ {
			cluster_manual_failover(&(*servers)[i])
			// In tsan, tendis will taken 2~4 seconds to perform failover.
			time.Sleep(5 * time.Second)
		}

		time.Sleep(5 * time.Second)

		for i := 0; i < clusterNodeNum; i++ {
			if isMaster(&(*servers)[i]) {
				log.Fatalf("node should be slave")
			}
			if !cluster_check_state(&(*servers)[i]) {
				log.Fatal("cluster state error, online failed")
			}
		}

		for i := clusterNodeNum; i < clusterNodeNum*2; i++ {
			if !isMaster(&(*servers)[i]) {
				log.Fatalf("node should be master")
			}
			if !cluster_check_state(&(*servers)[i]) {
				log.Fatal("cluster state error, online failed")
			}
		}

		// shutdown old master
		for i := 0; i < clusterNodeNum; i++ {
			shutdownServer(&(*servers)[i], *shutdown, *clear)
		}

		time.Sleep(10 * time.Second)

		for i := 0; i < clusterNodeNum; i++ {
			(*servers)[i].WithBinPath("")
			(*servers)[i].Start(false, fmt.Sprintf("%s/test.cfg", (*servers)[i].Path))
		}

		totalKeyNumber += <-channel

		for i := 0; i < clusterNodeNum; i++ {
			for {
				if cluster_check_repl_offset(&(*servers)[i+clusterNodeNum], &(*servers)[i]) {
					break
				} else {
					log.Info("cluster check repl offset not equal, wait")
					time.Sleep(1 * time.Second)
				}
			}
		}

		time.Sleep(5 * time.Second)

		oldMasterDbsize := 0
		for i := 0; i < clusterNodeNum; i++ {
			// check master and slave dbsize is the same
			size1 := getDbsize(&(*servers)[i])
			size2 := getDbsize(&(*servers)[i+clusterNodeNum])
			if size1 != size2 {
				log.Fatalf("checkDbsize failed i:%d size1:%d size2:%d", i, size1, size2)
			}
			oldMasterDbsize += size1
		}
		if oldMasterDbsize != totalKeyNumber {
			log.Fatalf("dbsize %v in tendis not equal to total input data %v.", oldMasterDbsize, totalKeyNumber)
		}

		for i := 0; i < clusterNodeNum*2; i++ {
			shutdownServer(&(*servers)[i], *shutdown, *clear)
		}
		shutdownPredixy(predixy, *shutdown, *clear)
		log.Infof("version: %v done.", v)
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
	testVersion(flag.Args())
	log.Infof("versiontest.go passed.")
}
