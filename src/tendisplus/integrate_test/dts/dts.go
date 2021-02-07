package main

import (
	"tendisplus/integrate_test/util"
	"bytes"
	"context"
	"flag"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
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
	mport = flag.Int("masterport", 62001, "master port")
	sport = flag.Int("slaveport", 62002, "slave port")
	tport = flag.Int("targetport", 62003, "target port")
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

func configSet(s *util.RedisServer, k string, v string) {
	cli := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer cli.Close()

	r, err := cli.ConfigSet(context.Background(), k, v).Result()
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
		if err := cli.Set(context.Background(), "mystr:"+uuid.New().String(), uuid.New().String(), 0).Err(); err != nil {
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
		if err := cli.ZAdd(context.Background(), "mysortedset:"+uuid.New().String(),
			&redis.Z{Score: float64(rand.Int()), Member: uuid.New().String()},
			&redis.Z{Score: float64(rand.Int()), Member: uuid.New().String()},
			&redis.Z{Score: float64(rand.Int()), Member: uuid.New().String()}).Err(); err != nil {
			log.Fatalf("zadd failed. %v", err)
		}
	}
}

func saddData(m *util.RedisServer) {
	cli := redis.NewClient(&redis.Options{
		Addr: m.Addr(),
	})
	defer cli.Close()

	for i := 0; i < 100; i++ {
		if err := cli.SAdd(context.Background(), "myset:"+uuid.New().String(),
			uuid.New().String(),
			uuid.New().String(),
			uuid.New().String()).Err(); err != nil {
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
		if err := cli.LPush(context.Background(), "mylist:"+strconv.Itoa(i),
			uuid.New().String(),
			uuid.New().String(),
			uuid.New().String()).Err(); err != nil {
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
		if err := cli.RPush(context.Background(), "mylist:"+strconv.Itoa(i),
			uuid.New().String(),
			uuid.New().String(),
			uuid.New().String()).Err(); err != nil {
			log.Fatalf("lpush failed. %v", err)
		}
	}
}

func hmsetData(m *util.RedisServer) {
	cli := redis.NewClient(&redis.Options{
		Addr: m.Addr(),
	})
	defer cli.Close()

	for i := 0; i < 100; i++ {
		if err := cli.HMSet(context.Background(), "myhash:"+uuid.New().String(),
			uuid.New().String(), uuid.New().String(),
			uuid.New().String(), uuid.New().String(),
			uuid.New().String(), uuid.New().String(),
			uuid.New().String(), uuid.New().String()).Err(); err != nil {
			log.Fatalf("mset failed. %v", err)
		}
	}
}

func otherData(m *util.RedisServer) {
	cli := redis.NewClient(&redis.Options{
		Addr: m.Addr(),
	})
	defer cli.Close()

	for i := 0; i < 10000; i++ {
		if err := cli.HSet(context.Background(), "", uuid.New().String(), uuid.New().String()).Err(); err != nil {
			log.Fatalf("insert data failed. %v", err)
		}
	}

	for i := 0; i < 10000; i++ {
		if err := cli.Set(context.Background(), uuid.New().String(), uuid.New().String(), time.Millisecond*time.Duration(rand.Int31n(1000))).Err(); err != nil {
			log.Fatalf("insert data failed. %v", err)
		}
	}

}

func specifHashData(m *util.RedisServer) {
	cli := redis.NewClient(&redis.Options{
		Addr: m.Addr(),
	})
	defer cli.Close()

	f := func(keyCount int, expiredTime time.Duration) {
		key := "myhash" + strconv.Itoa(keyCount) + "Expired" + expiredTime.String() + uuid.New().String()
		for i := 0; i < keyCount; i++ {
			if err := cli.HSet(context.Background(), key, uuid.New().String(), uuid.New().String()).Err(); err != nil {
				log.Fatalf("insert data failed. %v", err)
			}
		}
		cli.PExpire(context.Background(), key, expiredTime)
	}

	f(1000, time.Millisecond)
	f(1000, time.Minute)
	f(1000, time.Millisecond*time.Duration(rand.Int31n(1000)))

	f(999, time.Millisecond)
	f(999, time.Minute)
	f(999, time.Millisecond*time.Duration(rand.Int31n(1000)))

	f(1001, time.Millisecond)
	f(1001, time.Minute)
	f(1001, time.Millisecond*time.Duration(rand.Int31n(1000)))
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
	log.Printf("FindAvailablePort:%d", masterPort)

	m.Init("127.0.0.1", masterPort, pwd, "m_")

	if err := m.Setup(false, &cfgArgs); err != nil {
		log.Fatalf("setup master failed:%v", err)
	}
	defer shutdownServer(m)

	s := new(util.RedisServer)
	s.Ip = "127.0.0.1"
	s.WithBinPath("tendisplus")
	slavePort := util.FindAvailablePort(*sport)
	log.Printf("FindAvailablePort:%d", slavePort)

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
	log.Printf("FindAvailablePort:%d", targetPort)

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

	log.Println(stdoutDTS.String())
	log.Println(stderrDTS.String())

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

	log.Println(stdoutComp.String())
	log.Println(stderrComp.String())

	if strings.Contains(stdoutComp.String(), "error") {
		log.Fatalln(stdoutComp.String())
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

	log.Println(stdoutComp.String())
	log.Println(stderrComp.String())

	if strings.Contains(stdoutComp.String(), "error") {
		log.Fatalln(stdoutComp.String())
	}

}
