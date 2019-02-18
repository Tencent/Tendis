package main

import (
	"fmt"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/ngaut/log"
	"math/rand"
	"sync"
	"sync/atomic"
	"tendisplus/integrate_test/util"
	"time"
)

func testReplMatch2(m, s *util.RedisServer) {
	var wg sync.WaitGroup
	running := int32(1)
	buf := make(chan int)
	keys := []int{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cli, err := redis.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", m.Port), 10*time.Second)
			if err != nil {
				log.Fatalf("dial master failed:%v", err)
			}
			cnt := idx * 1000000000
			for atomic.LoadInt32(&running) == 1 {
				cnt += 1
				r, err := cli.Cmd("set", fmt.Sprintf("%d", cnt), fmt.Sprintf("%d", cnt)).Str()
				if err != nil {
					log.Fatalf("do set failed:%v", err)
				} else if r != "OK" {
					log.Fatalf("do set error:%s", r)
				}
				buf <- cnt
			}
		}(i)
	}

	var wg1 sync.WaitGroup
	go func() {
		wg1.Add(1)
		defer wg1.Done()
		for i := range buf {
			keys = append(keys, i)
		}
	}()

	time.Sleep(20 * time.Second)
	atomic.StoreInt32(&running, 0)
	log.Infof("close all producers")
	wg.Wait()
	log.Infof("all producers closed")
	close(buf)
	log.Infof("quque closed")
	wg1.Wait()
	log.Infof("consumer closed")

	cli1, err := redis.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", m.Port), 10*time.Second)
	if err != nil {
		log.Fatalf("dial master failed:%v", err)
	}
	cli2, err := redis.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", s.Port), 10*time.Second)
	if err != nil {
		log.Fatalf("dial slave failed:%v", err)
	}
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
	for _, o := range keys {
		r, err := cli2.Cmd("get", fmt.Sprintf("%d", o)).Str()
		if err != nil {
			log.Fatalf("do get on slave failed:%v", err)
		} else if r != fmt.Sprintf("%d", o) {
			log.Fatalf("do get on slave bad return:%s", r)
		}
	}
}

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
	time.Sleep(10 * time.Second)
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
	m.Init(22345, "m_")
	s.Init(22346, "s_")
	if err := m.Setup(false); err != nil {
		log.Fatalf("setup master failed:%v", err)
	}
	if err := s.Setup(false); err != nil {
		log.Fatalf("setup slave failed:%v", err)
	}
	testReplMatch1(&m, &s)
	testReplMatch2(&m, &s)
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	testRepl()
}
