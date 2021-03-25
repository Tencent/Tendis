// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

package main

import (
	"flag"
	"fmt"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/ngaut/log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"tendisplus/integrate_test/util"
	"time"
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

	num1         = flag.Int("num1", 100, "first add data nums")
	num2         = flag.Int("num2", 100, "first add data nums")
	shutdown     = flag.Int("shutdown", 1, "whether shutdown the dir")
	clear        = flag.Int("clear", 1, "whether clear the dir")
	startup      = flag.Int("startup", 1, "whether startup")
	iscompare    = flag.Int("compare", 1, "whether compare")
	kvstorecount = flag.Int("kvstorecount", 10, "kvstore count")
	auth         = flag.String("auth", "tendis+test", "password")
	keyprefix1   = flag.String("keyprefix1", "aa", "keyprefix1")
	keyprefix2   = flag.String("keyprefix2", "bb", "keyprefix2")
	// "set,incr,lpush,lpop,sadd,spop,hset,mset"
	benchtype = flag.String("benchtype", "set,incr,lpush,sadd,hset", "benchmark data type")
	valgrind  = flag.Bool("valgrind", false, "whether valgrind")
)

func getCurrentDirectory() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}
	return strings.Replace(dir, "\\", "/", -1)
}

func PortInUse(port int) bool {
	checkStatement := fmt.Sprintf("lsof -i:%d ", port)
	output, _ := exec.Command("sh", "-c", checkStatement).CombinedOutput()
	if len(output) > 0 {
		return true
	}
	return false
}

func addDataInCoroutine(m *util.RedisServer, num int, prefixkey string, channel chan int) {
	var optype string = *benchtype
	//log.Infof("optype is : %s", optype)
	switch optype {
	case "set":
		log.Infof("optype is : %s", optype)
		addData(m, num, prefixkey)
	case "sadd":
		log.Infof("optype is : %s", optype)
		addSetData(m, num, prefixkey)
	case "hmset":
		log.Infof("optype is : %s", optype)
		addHashData(m, num, prefixkey)
	case "rpush":
		log.Infof("optype is : %s", optype)
		addListData(m, num, prefixkey)
	case "zadd":
		log.Infof("optype is : %s", optype)
		addSortedData(m, num, prefixkey)
	default:
		log.Infof("no benchtype")
	}
	//addData(m, num, prefixkey)
	channel <- 0
}

func expireKey(m *util.RedisServer, num int, prefixkey string) {
	log.Infof("expireKey begin. %s:%d", m.Ip, m.Port)

	for i := 1; i < 1+num; i++ {
		var args []string
		args = append(args, "-h", m.Ip, "-p", strconv.Itoa(m.Port),
			"-c", "-a", *auth, "expire")

		key := ""
		if *benchtype == "set" {
			key = fmt.Sprintf("key:%04s%010d", prefixkey, i)
		} else {
			key = "key" + prefixkey + "_" + strconv.Itoa(i)
		}
		args = append(args, key)

		value := "50"
		args = append(args, value)

		cmd := exec.Command("../../../bin/redis-cli", args...)
		data, err := cmd.Output()
		//log.Infof("sadd:  %s", data)
		if string(data) != "1\n" || err != nil {
			log.Infof("sadd failed, key%v data:%s err:%v", key, data, err)
		}
	}
	log.Infof("expireKey end. %s:%d num:%d", m.Ip, m.Port, num)
}

func addData(m *util.RedisServer, num int, prefixkey string) {
	log.Infof("addData begin. %s:%d", m.Ip, m.Port)

	/*cmd := exec.Command("../../../bin/redis-benchmark", "-h", m.Ip, "-p", strconv.Itoa(m.Port),
	      "-c", "20", "-n", strconv.Itoa(num), "-r", "8", "-i", "-f", prefixkey,
	      "-t", *benchtype, "-a", *auth)
	  _, err := cmd.Output()
	  //fmt.Print(string(output))
	  if err != nil {
	      fmt.Print(err)
	  }*/
	logFilePath := fmt.Sprintf("benchmark_%d.log", m.Port)
	var cmd string
	cmd = fmt.Sprintf("../../../bin/redis-benchmark -h %s -p %d -c 20 -n %d -r 8 -i -f %s -t %s -a %s > %s 2>&1",
		m.Ip, m.Port, num, prefixkey, *benchtype, *auth, logFilePath)
	log.Infof("addData cmd:%s", cmd)
	args := []string{}
	args = append(args, cmd)
	inShell := true
	_, err := util.StartProcess(args, []string{}, "", 10*time.Second, inShell, nil)
	if err != nil {
		log.Fatalf("addData failed:%v", err)
		return
	}

	log.Infof("addData sucess. %s:%d num:%d", m.Ip, m.Port, num)
}

// sadd key elements [elements...]
func addSetData(m *util.RedisServer, num int, prefixkey string) {
	log.Infof("addData begin(Sadd). %s:%d", m.Ip, m.Port)

	for i := 0; i < num; i++ {
		var args []string
		args = append(args, "-h", m.Ip, "-p", strconv.Itoa(m.Port),
			"-c", "-a", *auth, "sadd")

		key := "key" + prefixkey + "_" + strconv.Itoa(i)
		args = append(args, key)
		for h := 0; h < 1; h++ {
			value := "value" + prefixkey + "_" + strconv.Itoa(h)
			args = append(args, value)
		}

		cmd := exec.Command("../../../bin/redis-cli", args...)
		data, err := cmd.Output()
		//log.Infof("sadd:  %s", data)
		if string(data) != "1\n" || err != nil {
			log.Infof("sadd failed, key%v data:%s err:%v", key, data, err)
		}
	}
	log.Infof("addData success(Sadd). %s:%d num:%d", m.Ip, m.Port, num)
}

func addHashData(m *util.RedisServer, num int, prefixkey string) {
	log.Infof("addData begin(Hmset). %s:%d", m.Ip, m.Port)

	for i := 0; i < num; i++ {
		var args []string
		args = append(args, "-h", m.Ip, "-p", strconv.Itoa(m.Port),
			"-c", "-a", *auth, "hmset")

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
		if string(data) != "OK\n" || err != nil {
			log.Infof("hmset failed, key%v data:%s err:%v", key, data, err)
		}
	}
	log.Infof("addData success(Hmset). %s:%d num:%d", m.Ip, m.Port, num)
}

func addListData(m *util.RedisServer, num int, prefixkey string) {
	log.Infof("addData begin(Rpush). %s:%d", m.Ip, m.Port)

	for i := 0; i < num; i++ {
		var args []string
		args = append(args, "-h", m.Ip, "-p", strconv.Itoa(m.Port),
			"-c", "-a", *auth, "rpush")

		key := "key" + prefixkey + "_" + strconv.Itoa(i)
		args = append(args, key)
		for h := 0; h < 1; h++ {
			value := "value" + prefixkey + "_" + strconv.Itoa(h)
			args = append(args, value)
		}

		cmd := exec.Command("../../../bin/redis-cli", args...)
		data, err := cmd.Output()
		//log.Infof("sadd:  %s", data)
		if string(data) != "1\n" || err != nil {
			log.Infof("Rpush failed, key%v data:%s err:%v", key, data, err)
		}
	}
	log.Infof("addData success(Rpush). %s:%d num:%d", m.Ip, m.Port, num)
}

func addSortedData(m *util.RedisServer, num int, prefixkey string) {
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

// format=="redis-benchmark": "key:{12}0000000001"
func checkDataInCoroutine(m *util.RedisServer, num int, prefixkey string, keyformat string,
	channel chan int) {
	var optype string = *benchtype
	//log.Infof("optype is : %s", optype)
	switch optype {
	case "set":
		log.Infof("optype is : %s", optype)
		checkData(m, num, prefixkey, keyformat)
	case "sadd":
		log.Infof("optype is : %s", optype)
		checkSetData(m, num, prefixkey)
	case "hmset":
		log.Infof("optype is : %s", optype)
		checkHashData(m, num, prefixkey)
	case "rpush":
		log.Infof("optype is : %s", optype)
		checkListData(m, num, prefixkey)
	case "zadd":
		log.Infof("optype is : %s", optype)
		checkSortedData(m, num, prefixkey)
	default:
		log.Infof("no benchtype")
	}
	//checkData(m, num, prefixkey, keyformat, onlyMyself)
	channel <- 0
}

func checkData(m *util.RedisServer, num int, prefixkey string, keyformat string) {
	log.Infof("checkData begin. num:%d prefixkey:%s keyformat:%s", num, prefixkey, keyformat)

	cli := createClient(m)
	for i := 1; i <= num+1; i++ {
		key := ""
		value := ""
		if keyformat == "redis-benchmark" {
			//"key:{12}0000000001"
			key = fmt.Sprintf("key:%04s%010d", prefixkey, i)
			value = "xxx"
		} else {
			// add if needed
		}

		var data string
		var err error
		data, err = cli.Cmd("get", key).Str()
		if err != nil {
			log.Infof("get failed, key:%v data:%s err:%v", key, data, err)
			return
		}

		retValue := strings.Replace(string(data), "\n", "", -1)
		if retValue != value {
			log.Infof("find failed, key:%v data:%s value:%s err:%v", key, retValue, value, err)
		}
	}
	log.Infof("checkData end. num:%d prefixkey:%s keyformat:%s", num, prefixkey, keyformat)
}

func checkSetData(m *util.RedisServer, num int, prefixkey string) {
	log.Infof("checkData begin(Sadd). prefixkey:%s", prefixkey)

	for i := 0; i < num; i++ {
		var args []string
		args = append(args, "-h", (*m).Ip, "-p", strconv.Itoa((*m).Port),
			"-c", "-a", *auth, "scard")

		key := "key" + prefixkey + "_" + strconv.Itoa(i)
		value := "1"
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

func checkHashData(m *util.RedisServer, num int, prefixkey string) {
	log.Infof("checkData begin(Hmset). prefixkey:%s", prefixkey)

	for i := 0; i < num; i++ {
		var args []string
		args = append(args, "-h", (*m).Ip, "-p", strconv.Itoa((*m).Port),
			"-c", "-a", *auth, "hlen")

		key := "key" + prefixkey + "_" + strconv.Itoa(i)
		value := "1"
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

func checkListData(m *util.RedisServer, num int, prefixkey string) {
	log.Infof("checkData begin(Rpush). prefixkey:%s", prefixkey)

	for i := 0; i < num; i++ {
		var args []string
		args = append(args, "-h", (*m).Ip, "-p", strconv.Itoa((*m).Port),
			"-c", "-a", *auth, "llen")

		key := "key" + prefixkey + "_" + strconv.Itoa(i)
		value := "1"
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

func checkSortedData(m *util.RedisServer, num int, prefixkey string) {
	log.Infof("checkData begin(Zadd). prefixkey:%s", prefixkey)

	for i := 0; i < num; i++ {
		var args []string
		args = append(args, "-h", (*m).Ip, "-p", strconv.Itoa((*m).Port),
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

func createClient(m *util.RedisServer) *redis.Client {
	cli, err := redis.DialTimeout("tcp", fmt.Sprintf("%s:%d", m.Ip, m.Port), 10*time.Second)
	if err != nil {
		log.Fatalf("can't connect to %s:%d err:%v", m.Ip, m.Port, err)
	}
	if *auth != "" {
		if v, err := cli.Cmd("AUTH", *auth).Str(); err != nil || v != "OK" {
			log.Fatalf("auth failed. %s:%d auth:%s", m.Ip, m.Port, *auth)
		}
	}
	return cli
}

func addOnekeyEveryStore(m *util.RedisServer, kvstorecount int) {
	cli := createClient(m)

	for i := 0; i < kvstorecount; i++ {
		if r, err := cli.Cmd("setinstore", strconv.Itoa(i), "fixed_test_key", "fixed_test_value").Str(); err != nil {
			log.Fatalf("do addOnekeyEveryStore %d failed:%v", i, err)
		} else if r != "OK" {
			log.Fatalf("do addOnekeyEveryStore error:%s", r)
			return
		}
	}
	log.Infof("addOnekeyEveryStore sucess.port:%d", m.Port)
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
			var binlogmax1 int
			if r, err := cli1.Cmd("binlogpos", i).Int(); err != nil {
				log.Fatalf("do waitCatchup %d failed:%v", i, err)
			} else {
				//log.Infof("binlogpos store:%d binlogmax:%d" , i, r)
				binlogmax1 = r
			}

			var binlogmax2 int
			if r, err := cli2.Cmd("binlogpos", i).Int(); err != nil {
				log.Fatalf("do waitCatchup %d failed:%v", i, err)
			} else {
				//log.Infof("binlogpos store:%d binlogmax:%d" , i, r)
				binlogmax2 = r
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
