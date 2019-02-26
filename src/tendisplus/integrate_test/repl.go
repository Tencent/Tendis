package main

import (
	"flag"
	"fmt"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/ngaut/log"
	"math/rand"
	"sync"
	"sync/atomic"
	"tendisplus/integrate_test/util"
	"time"
)

var (
	mport     = flag.Int("masterport", 0, "master port")
	sport     = flag.Int("slaveport", 0, "slave port")
	zsetcount = flag.Int("zsetcount", 1, "zset count")
	setcount  = flag.Int("setcount", 100, "set count")
	listcount = flag.Int("listcount", 100, "list count")
	hashcount = flag.Int("hashcount", 100, "hash count")
	kvcount   = flag.Int("kvcount", 100000, "kv count")
	threadnum = flag.Int("threadnum", 4, "thd count")
	loadsecs  = flag.Int("loadsecs", 20, "seconds for loading data")
)

type TendisType int

const (
	KV TendisType = iota
	SET
	ZSET
	LIST
	HASH
)

type Record struct {
	Pk   string
	Sk   string
	Type TendisType
}

func getRandomType() TendisType {
	typelist := []TendisType{KV, SET, ZSET}
	return typelist[rand.Int()%len(typelist)]
}

func testReplMatch2(m, s *util.RedisServer) {
	var wg sync.WaitGroup
	running := int32(1)
	buf := make(chan *Record)
	keys := []*Record{}
	for i := 0; i < *threadnum; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cli, err := redis.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", m.Port), 10*time.Second)
			if err != nil {
				log.Fatalf("dial master failed:%v", err)
			}
			for atomic.LoadInt32(&running) == 1 {
			    tp := getRandomType()
				if tp == KV {
					suffix := rand.Int() % (*kvcount)
					pk := fmt.Sprintf("kv%d", suffix)
					r, err := cli.Cmd("set", pk, fmt.Sprintf("%d", suffix)).Str()
					if err != nil {
						log.Fatalf("do set failed:%v", err)
					} else if r != "OK" {
						log.Fatalf("do set error:%s", r)
					}
					buf <- &Record{Pk: pk, Sk: "", Type: tp}
				} else if tp == SET {
					suffix := rand.Int() % (*setcount)
					pk := fmt.Sprintf("set%d", suffix)
					sk := fmt.Sprintf("sk%d", rand.Int()%10000)
					_, err := cli.Cmd("sadd", pk, sk).Int()
					if err != nil {
						log.Fatalf("do sadd failed:%v", err)
					}
					buf <- &Record{Pk: pk, Sk: sk, Type: tp}
				} else if tp == ZSET {
					suffix := rand.Int() % (*zsetcount)
					pk := fmt.Sprintf("zset%d", suffix)
					sk := fmt.Sprintf("sk%d", rand.Int()%20000000)
					_, err := cli.Cmd("zrem", pk, sk).Int()
                    if err != nil {
						log.Fatalf("do zrem %s %s failed:%v", pk, sk, err)
					}
					_, err = cli.Cmd("zadd", pk, rand.Int()%10000000, sk).Int()
					if err != nil {
						log.Fatalf("do zadd %s %s failed:%v,%d", pk, sk, err)
					}
				}
			}
		}(i)
	}

	var wg1 sync.WaitGroup
	go func() {
		wg1.Add(1)
		defer wg1.Done()
		for o := range buf {
			keys = append(keys, o)
		}
	}()

    time.Sleep(time.Duration(*loadsecs)*time.Second)
	atomic.StoreInt32(&running, 0)
	log.Infof("close all producers")
	wg.Wait()
	log.Infof("all producers closed")
	close(buf)
	log.Infof("queue closed")
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
		if o.Type == KV {
			r, err := cli2.Cmd("get", o.Pk).Str()
			if err != nil {
				log.Fatalf("do get on slave failed:%v", err)
			}
			r1, err := cli1.Cmd("get", o.Pk).Str()
			if err != nil {
				log.Fatalf("do get on master failed:%v", err)
			}
			if r != r1 {
				log.Fatalf("kv:%s not match", o.Pk)
			}
		} else if o.Type == SET {
			r, err := cli2.Cmd("sismember", o.Pk, o.Sk).Int()
			if err != nil {
				log.Fatalf("do sismember on slave failed:%s,%s,%v", o.Pk, o.Sk, err)
			}
			r1, err := cli1.Cmd("sismember", o.Pk, o.Sk).Int()
			if err != nil {
				log.Fatalf("do sismember on master failed:%v", err)
			}
			if r != r1 {
				log.Fatalf("sismember:%s,%s not match", o, o.Pk, o.Sk)
			}
		} else if o.Type == ZSET {
			r, err := cli2.Cmd("zscore", o.Pk, o.Sk).Str()
			if err != nil {
				log.Fatalf("do zscore on slave failed:%v", err)
			}
			r1, err := cli1.Cmd("zscore", o.Pk, o.Sk).Str()
			if err != nil {
				log.Fatalf("do zscore on master failed:%v", err)
			}
			if r != r1 {
				log.Fatalf("zscore:%s,%s not match", o.Pk, o.Sk)
			}
		}
	}
	log.Infof("compare complete")
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

func testRepl(m_port int, s_port int) {
	m := util.RedisServer{}
	s := util.RedisServer{}
	m.Init(m_port, "m_")
	s.Init(s_port, "s_")
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
	flag.Parse()
	rand.Seed(time.Now().UTC().UnixNano())
	testRepl(*mport, *sport)
}
