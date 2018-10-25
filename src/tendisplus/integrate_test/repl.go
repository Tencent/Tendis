package main

import (
    "fmt"
    "time"
    "math/rand"
	"github.com/ngaut/log"
    "github.com/mediocregopher/radix.v2/redis"
	"tendisplus/integrate_test/util"
)

func testReplMatch1(m, s *util.RedisServer) {
	cli1, err := redis.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", m.Port), 10*time.Second)
	cli2, err := redis.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", s.Port), 10*time.Second)

	if r, err := cli2.Cmd("slaveof", "127.0.0.1", fmt.Sprintf("%d", m.Port)).Str(); err != nil {
		log.Fatalf("do slaveof failed:%v", err)
	} else if r != "OK" {
		log.Fatalf("do slaveof error:%s", r)
	}

	for i := 0; i < 10000; i += 1 {
		r, err := cli1.Cmd("set", fmt.Sprintf("%d", i), fmt.Sprintf("%d", i)).Str()
		if err != nil {
			log.Fatalf("do set on master failed:%v", err)
		} else if r != "OK" {
			log.Fatalf("do set on master error:%s", r)
		}
	}
    time.Sleep(10*time.Second)
	mPos, err := cli1.Cmd("binlogpos", "0").Int64()
	if err != nil {
		log.Fatalf("binlogpos of master failed:%v", err)
	}
	for {
		sPos, err := cli2.Cmd("binlogpos", "0").Int64()
		if err != nil {
			log.Fatalf("binlogpos of slave failed:%v", err)
		}
		if sPos == mPos {
			log.Infof("m/s binlog matches")
			break
		} else {
			log.Infof("mpos:%d, spos:%d", mPos, sPos)
			time.Sleep(1 * time.Second)
		}
	}

	for i := 0; i < 10000; i += 1 {
		r, err := cli2.Cmd("get", fmt.Sprintf("%d", i)).Str()
		if err != nil {
			log.Fatalf("do get on slave failed:%v", err)
		} else if r != fmt.Sprintf("%d", i) {
			log.Fatalf("do get on slave bad return:%s", r)
		}
	}
}

func testRepl() {
	m := util.RedisServer{}
	s := util.RedisServer{}
	m.Init(12345, "m_")
	s.Init(12346, "s_")
	if err := m.Setup(false); err != nil {
		log.Fatalf("setup master failed:%v", err)
	}
	if err := s.Setup(false); err != nil {
		log.Fatalf("setup slave failed:%v", err)
	}
	testReplMatch1(&m, &s)
}

func main() {
    rand.Seed(time.Now().UTC().UnixNano())
	testRepl()
}
