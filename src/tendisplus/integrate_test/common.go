// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

package main

import (
	"flag"
	"fmt"
	"integrate_test/util"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mediocregopher/radix.v2/redis"
	"github.com/ngaut/log"
)

var (
	m1port = flag.Int("master1port", 61001, "master1 port")
	s1port = flag.Int("slave1port", 61002, "slave1 port")
	s2port = flag.Int("slave2port", 61003, "slave2 port")
	m2port = flag.Int("master2port", 61004, "master2 port")

	m1ip = flag.String("master1ip", "127.0.0.1", "master1 ip")
	s1ip = flag.String("slave1ip", "127.0.0.1", "slave1 ip")
	s2ip = flag.String("slave2ip", "127.0.0.1", "slave2 ip")
	m2ip = flag.String("master2ip", "127.0.0.1", "master2 ip")

	clusterPortStart = flag.Int("clusterPortStart", 21000, "clustertest port start")
	clusterIp        = flag.String("clusterIp", "127.0.0.1", "clustertest ip")
	clusterNodeNum   = flag.Int("clusterNodeNum", 5, "clustertest node num")

	num1     = flag.Int("num1", 100, "first add data nums")
	num2     = flag.Int("num2", 100, "first add data nums")
	shutdown = flag.Int("shutdown", 1, "whether shutdown the dir")
	// do not clear log file for AddressSanitizer/ThreadSanitizer info
	clear        = flag.Int("clear", 0, "whether clear the dir")
	startup      = flag.Int("startup", 1, "whether startup")
	iscompare    = flag.Int("compare", 1, "whether compare")
	kvstorecount = flag.Int("kvstorecount", 10, "kvstore count")
	auth         = flag.String("auth", "tendis+test", "password")
	keyprefix1   = flag.String("keyprefix1", "aa", "keyprefix1")
	keyprefix2   = flag.String("keyprefix2", "bb", "keyprefix2")
	valgrind     = flag.Bool("valgrind", false, "whether valgrind")
)

func createClient(m *util.RedisServer) *redis.Client {
	return util.CreateClientWithAuth(m, 10, *auth)
}

func createClientWithTimeout(m *util.RedisServer, timeout int) *redis.Client {
	return util.CreateClientWithAuth(m, timeout, *auth)
}

func backup(m *util.RedisServer, backup_mode string, dir string) {
	cli := createClient(m)

	if !strings.Contains(dir, "back") {
		log.Fatalf("dirname need contain back for safe.")
		return
	}
	os.RemoveAll(dir)
	os.Mkdir(dir, os.ModePerm)
	if r, err := cli.Cmd("backup", dir, backup_mode).Str(); err != nil {
		log.Fatalf("do backup failed:%v", err)
		return
	} else if r != "OK" {
		log.Fatalf("do backup error:%s", r)
		return
	}
	for {
		if r, err := cli.Cmd("info", "backup").Str(); err != nil {
			log.Fatalf("do backup failed:%v", err)
		} else {
			if strings.Contains(r, "current-backup-running:no") {
				break
			} else {
				time.Sleep(time.Millisecond * 100)
			}
		}
	}
	log.Infof("backup sucess,port:%d dir:%v", m.Port, dir)
}

func slaveof(m *util.RedisServer, s *util.RedisServer) {
	cli := createClient(s)

	if r, err := cli.Cmd("slaveof", m.Ip, strconv.Itoa(m.Port)).Str(); err != nil {
		log.Fatalf("do slaveof failed:%v", err)
		return
	} else if r != "OK" {
		log.Fatalf("do slaveof error:%s", r)
		return
	}
	log.Infof("slaveof sucess,mport:%d sport:%d", m.Port, s.Port)
}

func getDbsize(m *util.RedisServer) int {
	cli := createClient(m)
	r, err := cli.Cmd("dbsize").Int()
	if err != nil {
		log.Fatalf("cluster countkeysinslot failed:%v %s", err, r)
	}
	return r
}

func restoreBackup(m *util.RedisServer, dir string) {
	cli := createClient(m)

	if r, err := cli.Cmd("restorebackup", "all", dir, "force").Str(); err != nil {
		log.Fatalf("do restorebackup failed:%v", err)
	} else if r != "OK" {
		log.Fatalf("do restorebackup error:%s", r)
	}
	log.Infof("restorebackup sucess,port:%d", m.Port)
}

func waitFullsyncInCoroutine(s *util.RedisServer, kvstorecount int, channel chan int) {
	waitFullsync(s, kvstorecount)
	channel <- 0
}

func waitFullsync(s *util.RedisServer, kvstorecount int) {
	log.Infof("waitFullsync begin.sport:%d", s.Port)
	cli2 := createClient(s)

	for i := 0; i < kvstorecount; i++ {
		for {
			var replstatus int
			if r, err := cli2.Cmd("replstatus", i).Int(); err != nil {
				log.Fatalf("do waitFullsync %d failed:%v", i, err)
			} else {
				//log.Infof("binlogpos store:%d binlogmax:%d" , i, r)
				replstatus = r
			}

			if replstatus != 3 {
				time.Sleep(100 * 1000000) // 100ms
			} else {
				break
			}
		}
	}
	log.Infof("waitFullsync sucess.sport:%d", s.Port)
}

func waitCatchupInCoroutine(m *util.RedisServer, s *util.RedisServer, kvstorecount int, channel chan int) {
	waitCatchup(m, s, kvstorecount)
	channel <- 0
}

func waitCatchup(m *util.RedisServer, s *util.RedisServer, kvstorecount int) {
	log.Infof("waitCatchup begin.mport:%d sport:%d", m.Port, s.Port)
	cli1 := createClient(m)
	cli2 := createClient(s)

	var loop_time int = 0
	for {
		loop_time++
		var same bool = true
		var total1 int = 0
		var total2 int = 0
		for i := 0; i < kvstorecount; i++ {
			// slave should be checked first to ensure that master/slave
			// have the same binlogs. If we check the binlog on the master
			// first, there may be misjudgments due to subsequent data writing
			var binlogmax2 int
			if r, err := cli2.Cmd("binlogpos", i).Int(); err != nil {
				log.Fatalf("do waitCatchup %d failed:%v", i, err)
			} else {
				//log.Infof("binlogpos store:%d binlogmax:%d" , i, r)
				binlogmax2 = r
			}

			var binlogmax1 int
			if r, err := cli1.Cmd("binlogpos", i).Int(); err != nil {
				log.Fatalf("do waitCatchup %d failed:%v", i, err)
			} else {
				//log.Infof("binlogpos store:%d binlogmax:%d" , i, r)
				binlogmax1 = r
			}

			if binlogmax1 != binlogmax2 {
				same = false
			}
			if loop_time%50 == 0 {
				diff := binlogmax1 - binlogmax2
				total1 += binlogmax1
				total2 += binlogmax2
				log.Infof("waitCatchup.mport:%d sport:%d storeid:%d binlogmax1:%d binlogmax2:%d diff:%d",
					m.Port, s.Port, i, binlogmax1, binlogmax2, diff)
			}
		}
		if loop_time%50 == 0 {
			log.Infof("waitCatchup.mport:%d sport:%d total1:%d total2:%d total_diff:%d",
				m.Port, s.Port, total1, total2, total1-total2)
		}
		if same {
			break
		} else {
			time.Sleep(100 * 1000000) // 100ms
		}
	}
	log.Infof("waitCatchup sucess.mport:%d sport:%d", m.Port, s.Port)
}

func waitDumpBinlog(m *util.RedisServer, kvstorecount int) {
	cli := createClient(m)

	for i := 0; i < kvstorecount; i++ {
		for {
			var binlogmin int
			var binlogmax int
			if r, err := cli.Cmd("binlogpos", i).Int(); err != nil {
				log.Fatalf("do waitDumpBinlog %d failed:%v", i, err)
			} else {
				//log.Infof("binlogpos store:%d binlogmax:%d" , i, r)
				binlogmax = r
			}

			if r, err := cli.Cmd("binlogstart", i).Int(); err != nil {
				log.Fatalf("do waitDumpBinlog %d failed:%v", i, err)
			} else {
				//log.Infof("binlogpos store:%d binlogmin:%d" , i, r)
				binlogmin = r
			}
			if binlogmin != binlogmax {
				time.Sleep(100 * 1000000) // 100ms
			} else {
				break
			}
		}
	}
	log.Infof("waitDumpBinlog sucess.port:%d", m.Port)
}

func flushBinlog(m *util.RedisServer) {
	cli := createClient(m)

	if r, err := cli.Cmd("binlogflush", "all").Str(); err != nil {
		log.Fatalf("do flushBinlog failed:%v", err)
	} else if r != "OK" {
		log.Fatalf("do flushBinlog error:%s", r)
	}
	log.Infof("flushBinlog sucess,port:%d", m.Port)
}

func pipeRun(commands []*exec.Cmd) {
	for i := 1; i < len(commands); i++ {
		commands[i].Stdin, _ = commands[i-1].StdoutPipe()
	}
	// commands[len(commands)-1].Stdout = os.Stdout

	for i := 1; i < len(commands); i++ {
		err := commands[i].Start()
		if err != nil {
			panic(err)
		}
	}
	commands[0].Run()

	for i := 1; i < len(commands); i++ {
		err := commands[i].Wait()
		if err != nil {
			panic(err)
		}
	}
}

func restoreBinlog(m1 *util.RedisServer, m2 *util.RedisServer, kvstorecount int, endTs uint64) {
	var channel chan int = make(chan int)
	for i := 0; i < kvstorecount; i++ {
		go restoreBinlogInCoroutine(m1, m2, i, endTs, channel)
	}
	for i := 0; i < kvstorecount; i++ {
		<-channel
	}
	log.Infof("restoreBinlog sucess,port:%d", m2.Port)
}

func restoreBinlogInCoroutine(m1 *util.RedisServer, m2 *util.RedisServer,
	storeId int, endTs uint64, channel chan int) {
	cli := createClient(m2)

	var binlogPos int
	if r, err := cli.Cmd("binlogpos", storeId).Int(); err != nil {
		log.Fatalf("do restoreBinlog %d failed:%v", storeId, err)
	} else {
		//log.Infof("binlogpos store:%d binlogmax:%d" , storeId, r)
		binlogPos = r
	}

	subpath := m1.Path + "/dump/" + strconv.Itoa(storeId) + "/"
	files, _ := filepath.Glob(subpath + "binlog*.log")
	if len(files) <= 0 {
		log.Infof("restoreBinlogInCoroutine skip because no binlog file, path:%d", subpath)
		channel <- 0
		return
	}
	sort.Strings(files)

	for j := 0; j < len(files); j++ {
		var commands []*exec.Cmd
		commands = append(commands, exec.Command("../../../build/bin/binlog_tool",
			"--logfile="+files[j],
			"--mode=base64",
			"--start-position="+strconv.Itoa(binlogPos+1),
			"--end-datetime="+strconv.FormatUint(endTs, 10),
		))
		if *auth == "" {
			commands = append(commands, exec.Command("../../../bin/redis-cli",
				"-p", strconv.Itoa(m2.Port)))
		} else {
			commands = append(commands, exec.Command("../../../bin/redis-cli",
				"-p", strconv.Itoa(m2.Port), "-a", *auth))
		}
		pipeRun(commands)

		log.Infof("restoreBinlog sucess store:%d binlogPos:%d endtimestamp:%v file:%s",
			storeId, binlogPos, strconv.FormatUint(endTs, 10), path.Base(files[j]))
	}
	log.Infof("restoreBinlog sucess,port:%d storeid:%d", m2.Port, storeId)
	channel <- 0
}

func restoreBinlogEnd(m *util.RedisServer, kvstorecount int) {
	cli := createClient(m)
	for i := 0; i < kvstorecount; i++ {
		if r, err := cli.Cmd("restoreend", i).Str(); err != nil {
			log.Fatalf("do restoreend failed:%v", err)
		} else if r != "OK" {
			log.Fatalf("do restoreend error:%s", r)
		}
	}
	log.Infof("restoreBinlogEnd sucess,port:%d", m.Port)
}

func shutdownServer(m *util.RedisServer, shutdown int, clear int) {
	if shutdown <= 0 {
		return
	}
	cli := createClient(m)

	if r, err := cli.Cmd("shutdown").Str(); err != nil {
		log.Fatalf("do shutdown failed:%v", err)
	} else if r != "OK" {
		log.Fatalf("do shutdown error:%s", r)
	}
	if clear > 0 {
		m.Destroy()
	}
	log.Infof("shutdownServer server,port:%d", m.Port)
}

func checkServerPidFile(m *util.RedisServer) bool {
	_, err := os.Stat(m.Path + "/tendisplus.pid")
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func shutdownPredixy(m *util.Predixy, shutdown int, clear int) {
	if shutdown <= 0 {
		return
	}

	// NOTE(takenliu) -pgid will kill the group process
	syscall.Kill(-m.Pid, syscall.SIGKILL)
	if clear > 0 {
		m.Destroy()
	}
	log.Infof("shutdownPredixy server,port:%d pid:%d", m.Port, m.Pid)
}

func compareInCoroutine(m1 *util.RedisServer, m2 *util.RedisServer, channel chan int) {
	compare(m1, m2)
	channel <- 0
}

func compare(m1 *util.RedisServer, m2 *util.RedisServer) {
	cmd := exec.Command("../../../bin/compare_instances", "-addr1", fmt.Sprintf("%s:%d", m1.Ip, m1.Port), "-addr2", fmt.Sprintf("%s:%d", m2.Ip, m2.Port),
		"-password1", *auth, "-password2", *auth)
	cmd.Stderr = os.Stderr
	output, err := cmd.Output()
	fmt.Print("Command output:\n", string(output))
	if err != nil {
		fmt.Println("Command err:", err)
		log.Infof("compare failed.")
		return
	}
	log.Infof("compare sucess,port1:%d port2:%d", m1.Port, m2.Port)
}
func mapRedisType(redisType string) string {
	switch redisType {
	case "string":
		return "a"
	case "hash":
		return "H"
	case "set":
		return "S"
	case "list":
		return "L"
	case "zset":
		return "Z"
	default:
		return ""
	}
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
	if mapRedisType(kt) != keyType {
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
func checkFieldNumOrLen(fieldnum string, num int) error {
	num_scan, err := strconv.Atoi(fieldnum)
	if err != nil {
		return fmt.Errorf("parse fieldnum failed, fieldnum:%s, err:%v", fieldnum, err)
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
		fmt.Println(lines[i])
		if !checkBinlogLine((lines[i])) {
			log.Fatalf("binlog line format error, storeid:%d, line:%s", storeId, lines[i])
		}
		i++
		fmt.Println(lines[i])
		if !checkPrefix(lines[i], TxnPre) {
			log.Fatalf("binlog line format error, storeid:%d, line:%s", storeId, lines[i])
		}
		i++
		fmt.Println(lines[i])
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
