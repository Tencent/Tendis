// Copyright (C) 2025 THL A29 Limited, a Tencent company.  All rights reserved.
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

	"github.com/mediocregopher/radix.v2/redis"
	"github.com/ngaut/log"
)

var setCmdList = []string{
	"set", "hset", "sadd", "zadd", "lpush", "hmset",
}

var typeMap = map[string]string{
	"string": "a",
	"hash":   "H",
	"set":    "S",
	"list":   "L",
	"zset":   "Z",
}

var lenMap = map[string]string{
	"a": "strlen",
	"H": "hlen",
	"S": "scard",
	"L": "llen",
	"Z": "zcard",
}

func preCheckData(cli *redis.Client, dbid string, key string, keyType string, ttl string) error {
	cli.Cmd("select", dbid)
	// check key type
	kt, err1 := cli.Cmd("type", key).Str()
	if err1 != nil {
		return fmt.Errorf("get key type failed, key:%s, err:%v", key, err1)
	}
	if typeMap[kt] != keyType {
		return fmt.Errorf("key type not match, key:%s, redis_type:%s, scan_type:%s", key, kt, keyType)
	}

	ttl_actual, err2 := cli.Cmd("pttl", key).Int()
	if err2 != nil {
		return fmt.Errorf("get key pttl failed, key:%s, err:%v", key, err2)
	}
	// ttl in scan is timestamp, ttl in redis is remaining time
	if ttl_actual > 0 {
		expireTime := time.Now().UnixNano()/1e6 + int64(ttl_actual)
		ttl_scan, err3 := strconv.ParseInt(ttl, 10, 64)
		if err3 != nil {
			return fmt.Errorf("parse ttl failed, ttl:%s, err:%v", ttl, err3)
		}
		if expireTime < ttl_scan-1000 || expireTime > ttl_scan+1000 {
			return fmt.Errorf("key ttl not match, key:%s, redis_ttl:%d, scan_ttl:%s", key, expireTime, ttl)
		}
	} else if ttl_actual != -1 && ttl_actual != -2 {
		return fmt.Errorf("key ttl not match, key:%s, redis_ttl:%d, scan_ttl:%s", key, ttl_actual, ttl)
	}
	return nil
}
func checkFieldNumOrLen(numStr string, num int) error {
	num_scan, err := strconv.Atoi(numStr)
	if err != nil {
		return fmt.Errorf("parse fieldnum failed, fieldnum:%s, err:%v", numStr, err)
	}
	if num != num_scan {
		return fmt.Errorf("fieldnum not match, redis_fieldnum:%d, scan_fieldnum:%d", num, num_scan)
	}
	return nil
}

func checkData(serv *util.RedisServer, line string) error {
	if line == "" {
		return nil
	}
	parts := strings.Split(line, " ")
	if len(parts) < 6 {
		return fmt.Errorf("line format error, line:%s", line)
	}
	keyType := parts[0]
	dbid := parts[1]
	key := parts[2]
	ttl := parts[3]
	var len string
	if keyType == "a" {
		len = parts[4]
	} else {
		len = parts[5]
	}
	cli := createClient(serv)
	preCheckData(cli, dbid, key, keyType, ttl)
	// check key len/fieldnum
	l, err := cli.Cmd(lenMap[keyType], key).Int()

	if err != nil {
		return fmt.Errorf("get key len/fieldnum failed, key:%s, err:%v", key, err)
	}
	return checkFieldNumOrLen(len, l)
}

func scanData(serv *util.RedisServer, kvstorecount int) {
	var channel chan int = make(chan int)
	for i := 0; i < kvstorecount; i++ {
		go scanDataInCoroutine(serv, i, channel)
	}
	for i := 0; i < kvstorecount; i++ {
		<-channel
	}
	log.Info("scan data end")
}

func scanDataInCoroutine(serv *util.RedisServer, storeId int, channel chan int) {
	subpath := serv.Path + "/db/" + strconv.Itoa(storeId) + "/"
	output, err := exec.Command("../../../build/bin/ldb_tendis",
		"--db="+subpath,
		"tscan",
	).Output()
	if err != nil {
		log.Fatalf("scan data failed storeid:%d err:%v", storeId, err)
	}
	lines := strings.Split(string(output), "\n")

	for _, line := range lines {
		err := checkData(serv, line)
		if err != nil {
			log.Fatalf("check data failed, storeid:%d, line:%s, err:%v", storeId, line, err)
		}
	}
	log.Info("scan data sueccess storeid:", storeId)
	channel <- 0
}

func scanBinlog(serv *util.RedisServer, kvstorecount int) {
	var channel chan int = make(chan int)
	for i := 0; i < kvstorecount; i++ {
		go scanBinlogInCoroutine(serv, i, channel)
	}
	for i := 0; i < kvstorecount; i++ {
		<-channel
	}
	log.Info("scan binlog end")
}

var TxnPre = []string{
	"txnid", "slot", "ts", "cmd",
}
var OpPre = []string{
	"op", "pkey", "skey", "opvalue",
}

func checkBinlogLine(line string) bool {
	parts := strings.Split(line, " ")
	if len(parts) == 0 || parts[0] != "B" {
		return false
	}
	return true
}

func checkPrefix(line string, prefix []string) bool {
	parts := strings.Split(line, " ")
	if len(parts) != 6 && len(parts) < 8 {
		fmt.Println(("len not match"))
		return false
	}
	offset := 0
	for i := 0; i < len(parts); i++ {
		if parts[i] == "" {
			offset++
		} else {
			break
		}
	}
	for i := 0; i < 4; i++ {
		if !strings.HasPrefix(parts[i+offset], prefix[i]) {
			return false
		}
	}
	return true
}

// format:
// B 4294967041 1 0 0 73
// txnid:910 slot:15495 ts:1757588240893 cmd:set
// op:set pkey:a skey: opvalue:10
// op:set pkey b skey: opvalue:20
func scanBinlogInCoroutine(serv *util.RedisServer, storeId int, channel chan int) {
	subpath := serv.Path + "/db/" + strconv.Itoa(storeId) + "/"
	output, err := exec.Command("../../../build/bin/ldb_tendis",
		"--db="+subpath,
		"--column_family=binlog_cf",
		"tscan",
		"--printlog",
	).Output()
	if err != nil {
		log.Fatalf("scan binlog failed storeid:%d err:%v", storeId, err)
	}
	lines := strings.Split(string(output), "\n")
	// at least per three lines is a record
	i := 0
	for i < len(lines) {
		if i+2 >= len(lines) {
			log.Fatalf("binlog line format error, storeid:%d, line:%s", storeId, lines[i])
		}
		if !checkBinlogLine((lines[i])) {
			log.Fatalf("binlog line format error, storeid:%d, line:%s", storeId, lines[i])
		}
		i++
		if !checkPrefix(lines[i], TxnPre) {
			log.Fatalf("binlog line format error, storeid:%d, line:%s", storeId, lines[i])
		}
		i++
		if !checkPrefix(lines[i], OpPre) {
			log.Fatalf("binlog line format error, storeid:%d, line:%s", storeId, lines[i])
		}
		for {
			i++
			if i >= len(lines) || strings.HasPrefix(lines[i], "B ") {
				break
			}
		}
	}
	log.Info("scan binlog sueccess storeid:", storeId)
	channel <- 0
}

// test ldb_tendis
func testScan(portStart int, num int, commandType string) {
	*util.Optype = commandType

	kvstorecount := 2

	cfgArgs := make(map[string]string)
	cfgArgs["kvstorecount"] = strconv.Itoa(kvstorecount)
	cfgArgs["requirepass"] = "tendis+test"
	serv := util.StartSingleServer("serv", portStart, &cfgArgs)
	defer shutdownServer(serv, *shutdown, *clear)

	time.Sleep(10 * time.Second)

	// add data
	log.Infof("adddata begin")
	cli := util.CreateClientWithGoRedis(serv, *auth)
	for i := 0; i < len(setCmdList); i++ {
		util.AddTypeDataWithNum(cli, setCmdList[i], 0, num, 20, 1200000, 8, strconv.Itoa(i)+"_")
	}
	log.Infof("adddata end")

	time.Sleep(10 * time.Second)
	log.Infof("scanData begin")
	scanData(serv, kvstorecount)
	log.Infof("scanData end")
	log.Infof("scanBinlog begin")
	scanBinlog(serv, kvstorecount)
	log.Infof("scanBinlog end")

}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	flag.Parse()
	testScan(48000, 1000, "all")
	log.Infof("testScan.go passed.")
}
