package main

import (
	"github.com/google/uuid"
	"../util"
	"bytes"
	"context"
	"flag"
	"github.com/go-redis/redis"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var (
	mport        = flag.Int("masterport", 62001, "master port")
	sport        = flag.Int("slaveport", 62002, "slave port")
	tport        = flag.Int("targetport", 62003, "target port")
)

func shutdownServer(m *util.RedisServer) {
	cli := redis.NewClient(&redis.Options{
		Addr: m.Addr(),
	})
	defer cli.Close()

	err := cli.Shutdown(context.Background()).Err()
	if err != nil {
		log.Printf("can't connect to %d: %v", m.Port, err)
	}
	m.Destroy()
}

func slaveOf(m *util.RedisServer, s *util.RedisServer) {
	scli := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer scli.Close()

	r, err := scli.SlaveOf(context.Background(), m.Ip, strconv.Itoa(m.Port)).Result()
	if err != nil {
		log.Fatalf("do slaveof failed:%v", err)
	}

	if r != "OK" {
		log.Fatalf("do slaveof error:%s", r)
	}
}

func setData(m *util.RedisServer) {
	cli := redis.NewClient(&redis.Options{
		Addr: m.Addr(),
	})
	defer cli.Close()

	for i := 0; i < 100; i++ {
		if err := cli.Set(context.Background(), "set:"+ uuid.New().String(), uuid.New().String(), 0).Err(); err != nil {
			log.Fatalf("set failed. %v", err)
		}
	}
}

func zaddData(m *util.RedisServer) {
	cli := redis.NewClient(&redis.Options{
		Addr: m.Addr(),
	})
	defer cli.Close()

	for i := 0; i < 100; i++ {
		if err := cli.ZAdd(context.Background(), "zadd:"+uuid.New().String(), &redis.Z{Score: float64(i), Member: uuid.New().String()}).Err(); err != nil {
			log.Fatalf("zadd failed. %v", err)
		}
	}
}

func lpushData(m *util.RedisServer) {
	cli := redis.NewClient(&redis.Options{
		Addr: m.Addr(),
	})
	defer cli.Close()

	for i := 0; i < 100; i++ {
		if err := cli.LPush(context.Background(), "lpush:"+strconv.Itoa(i), uuid.New().String(), uuid.New().String(), uuid.New().String()).Err(); err != nil {
			log.Fatalf("lpush failed. %v", err)
		}
	}
}

func rpushData(m *util.RedisServer) {
	cli := redis.NewClient(&redis.Options{
		Addr: m.Addr(),
	})
	defer cli.Close()

	for i := 0; i < 100; i++ {
		if err := cli.RPush(context.Background(), "rpush:"+strconv.Itoa(i), uuid.New().String(), uuid.New().String(), uuid.New().String()).Err(); err != nil {
			log.Fatalf("lpush failed. %v", err)
		}
	}
}

func msetData(m *util.RedisServer) {
	cli := redis.NewClient(&redis.Options{
		Addr: m.Addr(),
	})
	defer cli.Close()

	for i := 0; i < 100; i++ {
		if err := cli.MSet(context.Background(), "mset:"+uuid.New().String(), uuid.New().String(), "mset:" + uuid.New().String(), uuid.New().String()).Err(); err != nil {
			log.Fatalf("mset failed. %v", err)
		}
	}
}

func writeData(m *util.RedisServer) {
	setData(m)
	zaddData(m)
	lpushData(m)
	rpushData(m)
	msetData(m)
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
	pwd := getCurrentDirectory()

	m := new(util.RedisServer)
	m.WithBinPath("../../../../build/bin/tendisplus")
	m.Ip = "127.0.0.1"
	masterPort := util.FindAvailablePort(*mport)
	log.Printf("FindAvailablePort:%d", masterPort)

	m.Init("127.0.0.1", masterPort, pwd, "m_")

	if err := m.Setup(false, &cfgArgs); err != nil {
		log.Fatalf("setup master failed:%v", err)
	}
	defer shutdownServer(m)


	s := new(util.RedisServer)
	s.Ip = "127.0.0.1"
	s.WithBinPath("../../../../build/bin/tendisplus")
	slavePort := util.FindAvailablePort(*sport)
	log.Printf("FindAvailablePort:%d", slavePort)

	s.Init("127.0.0.1", slavePort, pwd, "s_")

	if err := s.Setup(false, &cfgArgs); err != nil {
		log.Fatalf("setup slave failed:%v", err)
	}
	defer shutdownServer(s)

	slaveOf(m,s)

	t := new(util.RedisServer)
	t.Ip = "127.0.0.1"
	t.WithBinPath("../../../../build/bin/tendisplus")
	targetPort := util.FindAvailablePort(*tport)
	log.Printf("FindAvailablePort:%d", targetPort)

	t.Init("127.0.0.1", targetPort, pwd, "t_")

	delete(cfgArgs,"aof-enabled")
	if err := t.Setup(false, &cfgArgs); err != nil {
		log.Fatalf("setup target failed:%v", err)
	}
	defer shutdownServer(t)

	time.Sleep(15 * time.Second)

	writeData(m)

	var stdoutDTS bytes.Buffer
	var stderrDTS bytes.Buffer
	cmdDTS := exec.Command("../../../../bin/checkdts", s.Addr(),"", t.Addr(), "", "0", "1", "8000", "0", "0")
	cmdDTS.Stdout = &stdoutDTS
	cmdDTS.Stderr = &stderrDTS
	err := cmdDTS.Run()
	if err != nil {
		log.Fatal(err)
	}
	//defer cmdDTS.Process.Kill()

	writeData(m)

	log.Println(stdoutDTS.String())
	log.Println(stderrDTS.String())

	time.Sleep(time.Second*30)

	var stdoutComp bytes.Buffer
	var stderrComp bytes.Buffer
	cmdComp := exec.Command("../../../../bin/compare_instances", "-addr1",m.Addr(), "-addr2", t.Addr(),"-storeNum", "1")
	cmdComp.Stdout = &stdoutComp
	cmdComp.Stderr = &stderrComp
	err = cmdComp.Run()
	if err != nil {
		log.Fatal(err)
	}
	//defer cmdComp.Process.Kill()

	log.Println(stdoutComp.String())
	log.Println(stderrComp.String())

	if strings.Contains(stdoutComp.String(), "error") {
		log.Fatalln(stdoutComp.String())
	}

}
