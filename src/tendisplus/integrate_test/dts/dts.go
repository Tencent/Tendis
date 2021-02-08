package main

import (
	"bytes"
	"flag"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/ngaut/log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"tendisplus/integrate_test/util"
	"time"
)

var (
	mport = flag.Int("masterport", 62001, "master port")
	sport = flag.Int("slaveport", 62002, "slave port")
	tport = flag.Int("targetport", 62003, "target port")
)

func shutdownServer(m *util.RedisServer) {
	cli, err := redis.DialTimeout("tcp", m.Addr(), 10*time.Second)
	if err != nil {
		log.Fatalf("can't connect to %d: %v", m.Port, err)
	}

	defer cli.Close()

	if err := cli.Cmd("shutdown").Err; err != nil {
		log.Infof("can't connect to %d: %v", m.Port, err)
	}

	m.Destroy()
}

func slaveOf(m *util.RedisServer, s *util.RedisServer) {
	cli, err := redis.DialTimeout("tcp", s.Addr(), 10*time.Second)
	if err != nil {
		log.Fatalf("can't connect to %d: %v", s.Port, err)
	}

	defer cli.Close()

	r, err := cli.Cmd("slaveof", m.Ip, strconv.Itoa(m.Port)).Str()
	if err != nil {
		log.Fatalf("do slaveof failed:%v", err)
	}

	if r != "OK" {
		log.Fatalf("do slaveof error:%s", r)
	}
}

func configSet(s *util.RedisServer, k string, v string) {
	cli, err := redis.DialTimeout("tcp", s.Addr(), 10*time.Second)
	if err != nil {
		log.Fatalf("can't connect to %d: %v", s.Port, err)
	}

	defer cli.Close()

	r, err := cli.Cmd("config", "set", k, v).Str()
	if err != nil {
		log.Fatalf("do configset failed:%v", err)
	}

	if r != "OK" {
		log.Fatalf("do configset error:%s", r)
	}
}

func setData(m *util.RedisServer) {
	cli, err := redis.DialTimeout("tcp", m.Addr(), 10*time.Second)
	if err != nil {
		log.Fatalf("can't connect to %d: %v", m.Port, err)
	}

	defer cli.Close()

	for i := 0; i < 100; i++ {
		if err := cli.Cmd("set", "mystr:"+util.RandStrAlpha(30), util.RandStrAlpha(30)).Err; err != nil {
			log.Fatalf("set failed. %v", err)
		}
	}
}

func zaddData(m *util.RedisServer) {
	cli, err := redis.DialTimeout("tcp", m.Addr(), 10*time.Second)
	if err != nil {
		log.Fatalf("can't connect to %d: %v", m.Port, err)
	}

	defer cli.Close()

	for i := 0; i < 100; i++ {
		if err := cli.Cmd("zadd", "mysortedset:"+util.RandStrAlpha(30),
			float64(rand.Int()), util.RandStrAlpha(30),
			float64(rand.Int()), util.RandStrAlpha(30),
			float64(rand.Int()), util.RandStrAlpha(30)).Err; err != nil {
			log.Fatalf("zadd failed. %v", err)
		}
	}
}

func saddData(m *util.RedisServer) {
	cli, err := redis.DialTimeout("tcp", m.Addr(), 10*time.Second)
	if err != nil {
		log.Fatalf("can't connect to %d: %v", m.Port, err)
	}

	defer cli.Close()

	for i := 0; i < 100; i++ {
		if err := cli.Cmd("sadd", "myset:"+util.RandStrAlpha(30),
			util.RandStrAlpha(30),
			util.RandStrAlpha(30),
			util.RandStrAlpha(30)).Err; err != nil {
			log.Fatalf("zadd failed. %v", err)
		}
	}
}

func lpushData(m *util.RedisServer) {
	cli, err := redis.DialTimeout("tcp", m.Addr(), 10*time.Second)
	if err != nil {
		log.Fatalf("can't connect to %d: %v", m.Port, err)
	}

	defer cli.Close()

	for i := 0; i < 100; i++ {
		if err := cli.Cmd("lpush", "mylist:"+strconv.Itoa(i),
			util.RandStrAlpha(30),
			util.RandStrAlpha(30),
			util.RandStrAlpha(30)).Err; err != nil {
			log.Fatalf("lpush failed. %v", err)
		}
	}
}

func rpushData(m *util.RedisServer) {
	cli, err := redis.DialTimeout("tcp", m.Addr(), 10*time.Second)
	if err != nil {
		log.Fatalf("can't connect to %d: %v", m.Port, err)
	}

	defer cli.Close()

	for i := 0; i < 100; i++ {
		if err := cli.Cmd("rpush", "mylist:"+strconv.Itoa(i),
			util.RandStrAlpha(30),
			util.RandStrAlpha(30),
			util.RandStrAlpha(30)).Err; err != nil {
			log.Fatalf("lpush failed. %v", err)
		}
	}
}

func hmsetData(m *util.RedisServer) {
	cli, err := redis.DialTimeout("tcp", m.Addr(), 10*time.Second)
	if err != nil {
		log.Fatalf("can't connect to %d: %v", m.Port, err)
	}

	defer cli.Close()

	for i := 0; i < 100; i++ {
		if err := cli.Cmd("hmset", "myhash:"+util.RandStrAlpha(30),
			util.RandStrAlpha(30), util.RandStrAlpha(30),
			util.RandStrAlpha(30), util.RandStrAlpha(30),
			util.RandStrAlpha(30), util.RandStrAlpha(30),
			util.RandStrAlpha(30), util.RandStrAlpha(30)).Err; err != nil {
			log.Fatalf("mset failed. %v", err)
		}
	}
}

func otherData(m *util.RedisServer) {
	cli, err := redis.DialTimeout("tcp", m.Addr(), 10*time.Second)
	if err != nil {
		log.Fatalf("can't connect to %d: %v", m.Port, err)
	}

	defer cli.Close()

	for i := 0; i < 10000; i++ {
		if err := cli.Cmd("hset", "", util.RandStrAlpha(30), util.RandStrAlpha(30)).Err; err != nil {
			log.Fatalf("insert data failed. %v", err)
		}
	}

	for i := 0; i < 10000; i++ {
		if err := cli.Cmd("set", util.RandStrAlpha(30), util.RandStrAlpha(30), "PX", rand.Int31n(1000)+1).Err; err != nil {
			log.Fatalf("insert data failed. %v", err)
		}
	}

}

func specifHashData(m *util.RedisServer) {
	cli, err := redis.DialTimeout("tcp", m.Addr(), 10*time.Second)
	if err != nil {
		log.Fatalf("can't connect to %d: %v", m.Port, err)
	}

	defer cli.Close()

	f := func(keyCount int, expiredTime int) {
		key := "myhash" + strconv.Itoa(keyCount) + "Expired" + strconv.Itoa(expiredTime) + util.RandStrAlpha(30)
		for i := 0; i < keyCount; i++ {
			if err := cli.Cmd("hset", key, util.RandStrAlpha(30), util.RandStrAlpha(30)).Err; err != nil {
				log.Fatalf("insert data failed. %v", err)
			}
		}

		if err := cli.Cmd("pexpire", key, expiredTime+1).Err; err != nil {
			log.Fatalf("insert data failed. %v", err)
		}
	}

	f(1000, 1)
	f(1000, 60*1000)
	f(1000, int(rand.Int31n(1000)))

	f(999, 1)
	f(999, 60*1000)
	f(999, int(rand.Int31n(1000)))

	f(1001, 1)
	f(1001, 60*1000*1000)
	f(1001, int(rand.Int31n(1000)))
}

func writeData(m *util.RedisServer) {
	setData(m)
	zaddData(m)
	saddData(m)
	lpushData(m)
	rpushData(m)
	hmsetData(m)
	specifHashData(m)
	otherData(m)
}

func getCurrentDirectory() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}
	return strings.Replace(dir, "\\", "/", -1)
}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UTC().UnixNano())

	cfgArgs := make(map[string]string)
	cfgArgs["aof-enabled"] = "yes"
	cfgArgs["kvStoreCount"] = "1"
	cfgArgs["noexpire"] = "false"
	cfgArgs["generallog"] = "true"
	pwd := getCurrentDirectory()

	m := new(util.RedisServer)
	m.WithBinPath("tendisplus")
	m.Ip = "127.0.0.1"
	masterPort := util.FindAvailablePort(*mport)
	log.Infof("FindAvailablePort:%d", masterPort)

	m.Init("127.0.0.1", masterPort, pwd, "m_")

	if err := m.Setup(false, &cfgArgs); err != nil {
		log.Fatalf("setup master failed:%v", err)
	}
	defer shutdownServer(m)

	s := new(util.RedisServer)
	s.Ip = "127.0.0.1"
	s.WithBinPath("tendisplus")
	slavePort := util.FindAvailablePort(*sport)
	log.Infof("FindAvailablePort:%d", slavePort)

	s.Init("127.0.0.1", slavePort, pwd, "s_")

	if err := s.Setup(false, &cfgArgs); err != nil {
		log.Fatalf("setup slave failed:%v", err)
	}
	defer shutdownServer(s)

	slaveOf(m, s)

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
	defer shutdownServer(t)

	time.Sleep(15 * time.Second)

	writeData(m)

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

	writeData(m)

	log.Info(stdoutDTS.String())
	log.Info(stderrDTS.String())

	time.Sleep(time.Second * 30)

	var stdoutComp bytes.Buffer
	var stderrComp bytes.Buffer
	cmdComp := exec.Command("compare_instances", "-addr1", s.Addr(), "-addr2", t.Addr(), "-storeNum", "1")
	cmdComp.Stdout = &stdoutComp
	cmdComp.Stderr = &stderrComp
	err = cmdComp.Run()
	if err != nil {
		log.Fatal(err)
	}
	//defer cmdComp.Process.Kill()

	log.Info(stdoutComp.String())
	log.Info(stderrComp.String())

	if strings.Contains(stdoutComp.String(), "error") {
		log.Fatal(stdoutComp.String())
	}

	configSet(s, "noexpire", "false")
	cmdComp = exec.Command("compare_instances", "-addr1", t.Addr(), "-addr2", s.Addr(), "-storeNum", "1")
	cmdComp.Stdout = &stdoutComp
	cmdComp.Stderr = &stderrComp
	err = cmdComp.Run()
	if err != nil {
		log.Fatal(err)
	}
	//defer cmdComp.Process.Kill()

	log.Info(stdoutComp.String())
	log.Info(stderrComp.String())

	if strings.Contains(stdoutComp.String(), "error") {
		log.Fatal(stdoutComp.String())
	}

	log.Infof("dts.go passed.")

}
