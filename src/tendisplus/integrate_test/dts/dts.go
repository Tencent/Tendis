package main

import (
	"bytes"
	"flag"
	"math/rand"
	"os/exec"
	"tendisplus/integrate_test/util"
	"time"

	"github.com/ngaut/log"
)

var (
	mport = flag.Int("masterport", 62001, "master port")
	sport = flag.Int("slaveport", 62002, "slave port")
	tport = flag.Int("targetport", 62003, "target port")
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	flag.Parse()
	rand.Seed(time.Now().UTC().UnixNano())

	cfgArgs := make(map[string]string)
	cfgArgs["aof-enabled"] = "yes"
	cfgArgs["kvStoreCount"] = "1"
	cfgArgs["noexpire"] = "false"
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

	t := new(util.RedisServer)
	t.Ip = "127.0.0.1"
	t.WithBinPath("tendisplus")
	targetPort := util.FindAvailablePort(*tport)
	log.Infof("FindAvailablePort:%d", targetPort)

	t.Init("127.0.0.1", targetPort, pwd, "t_")

	cfgArgs["aof-enabled"] = "false"
	cfgArgs["noexpire"] = "yes"
	if err := t.Setup(false, &cfgArgs); err != nil {
		log.Fatalf("setup target failed:%v", err)
	}
	defer util.ShutdownServer(t)

	time.Sleep(15 * time.Second)

	util.WriteData(m)

	var stdoutDTS bytes.Buffer
	var stderrDTS bytes.Buffer
	cmdDTS := exec.Command("checkdts", m.Addr(), "", t.Addr(), "", "0", "1", "8000", "0", "0")
	cmdDTS.Stdout = &stdoutDTS
	cmdDTS.Stderr = &stderrDTS
	err := cmdDTS.Run()
	if err != nil {
		log.Fatal(err)
	}
	//defer cmdDTS.Process.Kill()

	util.WriteData(m)

	log.Info(stdoutDTS.String())
	log.Info(stderrDTS.String())

	time.Sleep(time.Second * 40)

	util.CompareData(s.Addr(), t.Addr(), 1)

	util.ConfigSet(s, "noexpire", "false")
	time.Sleep(time.Second * 5)
	util.CompareData(t.Addr(), s.Addr(), 1)

	time.Sleep(10 * time.Second)
	util.CompareData(m.Addr(), t.Addr(), 1)

	util.CompareData(m.Addr(), s.Addr(), 1)

	log.Infof("dts.go passed.")

}
