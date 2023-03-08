package main

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"tendisplus/integrate_test/util"
	"time"

	"github.com/mediocregopher/radix.v2/redis"
	"github.com/ngaut/log"
)

var (
	mport = flag.Int("masterport", 62001, "master port")
	sport = flag.Int("slaveport", 62002, "slave port")
	tport = flag.Int("targetport", 62003, "target port")
	xport = flag.Int("syncport", 62004, "redis-sync port")
)

func runRedisSync(port int, srcPort int, dstPort int, kvstore int) {
	conf_name := fmt.Sprintf("./redis-sync-%d.conf", kvstore)
	f, err := os.Create(conf_name)
	if err != nil {
		log.Fatalf("Create redis-sync conf error:%v", err)
	}

	// set redis-sync specific configuration
	var conf strings.Builder
	conf.WriteString("[server]\n")
	conf.WriteString(fmt.Sprintf("logfile=./sync-%d.log\n", kvstore))
	conf.WriteString("threads=10\n")
	conf.WriteString("skip-start=yes\n")
	conf.WriteString(fmt.Sprintf("kvstore=%d\n", kvstore))
	conf.WriteString("loglevel=debug\n")
	conf.WriteString("test-mode=yes\n")
	conf.WriteString("mode=redis\n")
	conf.WriteString(fmt.Sprintf("port=%d\n", port))
	conf.WriteString("\n")
	conf.WriteString("[source]\n")
	conf.WriteString(fmt.Sprintf("127.0.0.1:%d|\n", srcPort))
	conf.WriteString("\n")
	conf.WriteString("[remote]\n")
	conf.WriteString(fmt.Sprintf("127.0.0.1:%d|\n", dstPort))

	if _, err := f.WriteString(conf.String()); err != nil {
		log.Fatalf("Write redis-sync conf file error:%v", err)
	}

	var stdoutSync bytes.Buffer
	var stderrSync bytes.Buffer
	cmdSync := exec.Command("redis-sync", "-f", conf_name)
	cmdSync.Stdout = &stdoutSync
	cmdSync.Stderr = &stderrSync
	if err := cmdSync.Start(); err != nil {
		log.Fatal(fmt.Sprint(err) + ":" + stderrSync.String())
	}

	time.Sleep(2 * time.Second)
	cli, err := redis.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 30*time.Second)
	if err != nil {
		log.Fatalf("can't connect to redis-sync[%s] => %v", fmt.Sprintf("127.0.0.1:%d", port), err)
	}
	defer cli.Close()

	if reply, _ := cli.Cmd("syncadmin", "start").Str(); reply != "OK" {
		log.Fatalf("redis-sync start error, reply is %s", reply)
	} else {
		log.Debugf("redis-sync start successfully, \"SYNCADMIN start\" => reply[%s]", reply)
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	cfgArgs := make(map[string]string)
	cfgArgs["aof-enabled"] = "yes"
	cfgArgs["kvStoreCount"] = "10"
	cfgArgs["noexpire"] = "true"
	cfgArgs["generallog"] = "true"
	pwd := util.GetCurrentDirectory()

	m := new(util.RedisServer)
	m.WithBinPath("tendisplus")
	m.Ip = "127.0.0.1"
	masterPort := util.FindAvailablePort(*mport)
	log.Infof("FindAvailablePort:%d", masterPort)

	m.Init("127.0.0.1", masterPort, pwd, "m_")

	if err := m.Setup(false, &cfgArgs); err != nil {
		log.Fatalf("setup master failed:%v", err)
	}
	defer util.ShutdownServer(m)

	s := new(util.RedisServer)
	s.Ip = "127.0.0.1"
	s.WithBinPath("tendisplus")
	slavePort := util.FindAvailablePort(*sport)
	log.Infof("FindAvailablePort:%d", slavePort)

	s.Init("127.0.0.1", slavePort, pwd, "s_")

	if err := s.Setup(false, &cfgArgs); err != nil {
		log.Fatalf("setup slave failed:%v", err)
	}
	defer util.ShutdownServer(s)

	util.SlaveOf(m, s)

	targetPort := util.FindAvailablePort(*tport)
	log.Infof("FindAvailablePort:%d", targetPort)

	var stdoutRedis, stderrRedis bytes.Buffer
	cmdRedis := exec.Command("redis-server", fmt.Sprintf("--port %d", targetPort))
	cmdRedis.Stdout = &stdoutRedis
	cmdRedis.Stderr = &stderrRedis
	err := cmdRedis.Start()
	if err != nil {
		log.Fatal(fmt.Sprint(err) + ":" + stderrRedis.String())
	}
	defer cmdRedis.Process.Kill()
	time.Sleep(1 * time.Second)

	cli, err := redis.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", targetPort), 30*time.Second)
	if err != nil {
		log.Fatalf("can't connect to redis-server[%s] => %v", fmt.Sprintf("127.0.0.1:%d", targetPort), err)
	}

	if reply, _ := cli.Cmd("PING").Str(); reply != "PONG" {
		log.Fatalf("redis-server start error, reply is %s", reply)
	} else {
		log.Debugf("redis-server start successfully, \"PING\" => reply[%s]", reply)
	}
	cli.Close()

	time.Sleep(15 * time.Second)

	util.WriteData(m)
	log.Infof("Wrtite data Ok!, master-port:%d", masterPort)

	for i := 0; i < 10; i++ {
		syncPort := util.FindAvailablePort(*xport)
		log.Infof("FindAvailablePort:%d", syncPort)
		runRedisSync(syncPort, masterPort, targetPort, i)
	}

	util.WriteData(m)
	// wait slave catch up master
	time.Sleep(time.Second * 10)
	// m and s no expire now
	util.CompareData(s.Addr(), fmt.Sprintf("127.0.0.1:%d", targetPort), 1)
	util.CompareData(m.Addr(), fmt.Sprintf("127.0.0.1:%d", targetPort), 1)

	util.ConfigSet(m, "noexpire", "false")
	util.ConfigSet(s, "noexpire", "false")
	time.Sleep(time.Second * 5)
	// still no expire
	util.CompareData(s.Addr(), fmt.Sprintf("127.0.0.1:%d", targetPort), 1)
	util.CompareData(m.Addr(), fmt.Sprintf("127.0.0.1:%d", targetPort), 1)

	// wait for expire
	// see SpecifHashData for more info.
	time.Sleep(time.Second * 120)
	// after expire, m s redis should have same data.
	util.CompareData(m.Addr(), fmt.Sprintf("127.0.0.1:%d", targetPort), 1)

	util.CompareData(m.Addr(), s.Addr(), 1)

	util.CompareData(s.Addr(), fmt.Sprintf("127.0.0.1:%d", targetPort), 1)

	log.Infof("dts_sync.go passed.")
}
