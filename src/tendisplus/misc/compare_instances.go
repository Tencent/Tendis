// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

package main

import (
	"flag"
	"fmt"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/ngaut/log"
	"strconv"
	"sync"
	"time"
)

const (
	KV         = 2
	LIST_ELE   = 4
	HASH_ELE   = 6
	SET_ELE    = 8
	ZSET_H_ELE = 11
)

var (
	addr1     = flag.String("addr1", "127.0.0.1:10001", "addr1 host")
	addr2     = flag.String("addr2", "127.0.0.1:10002", "addr2 host")
	password1 = flag.String("password1", "", "password1")
	password2 = flag.String("password2", "", "password2")
	storeNum = flag.Int("storeNum", 10, "store number")
)

func main() {
	flag.Parse()
	var channel chan int = make(chan int)
	for i := 0; i < *storeNum; i++ {
		go processOneStore(i, *addr1, *addr2, channel)
	}
	var total int = 0
	for i := 0; i < *storeNum; i++ {
		num := <-channel
		total += num
	}
	fmt.Printf("total %d records compared.b:%s f:%s\n", total, *addr1, *addr2)
}

func procCoroutinue(procid int, addr2 string, jobs <-chan []*redis.Resp,
	result *[]int, result_lock *sync.Mutex, wg *sync.WaitGroup) {
	fe, err := redis.DialTimeout("tcp", addr2, 20*time.Second)
	if err != nil {
		log.Fatalf("dial %s failed:%v", addr2, err)
	}
	if *password2 != "" {
		if v, err := fe.Cmd("AUTH", *password2).Str(); err != nil || v != "OK" {
			log.Fatalf("auth %s failed", addr2)
		}
	}

	for job := range jobs {
		ret := procOneJob(procid, job, fe)

		result_lock.Lock()
		*result = append(*result, ret)
		result_lock.Unlock()
		wg.Done()
	}
}

func procOneJob(procid int, arr1 []*redis.Resp, fe *redis.Client) int {
	cnt := 0
	for _, o := range arr1 {
		arr2, err := o.Array()
		if err != nil {
			log.Fatalf("parse into record failed:%v", err)
		}
		dbid, _ := arr2[1].Str()
		if _, err := fe.Cmd("SELECT", dbid).Str(); err != nil {
			log.Fatalf("select %s failed %v", dbid, err)
		}
		types, _ := arr2[0].Str()
		key, _ := arr2[2].Str()
		subkey, _ := arr2[3].Str()
		val, _ := arr2[4].Str()
		typ, _ := strconv.ParseInt(types, 10, 64)
		if typ == KV {
			if v, err := fe.Cmd("GET", key).Str(); err != nil {
				log.Fatalf("get addr2:%s failed:%v", key, err)
			} else if v != val {
				log.Errorf("key:%s, addr2:%s, back:%s", key, v, val)
			}
		} else if typ == LIST_ELE {
			if v, err := fe.Cmd("LINDEX", key, subkey).Str(); err != nil {
				log.Fatalf("lindex addr2:%s:%s failed:%v", key, subkey, err)
			} else if v != val {
				log.Errorf("list key:%s, index:%s, addr2:%s, back:%s", key, subkey, v, val)
			}
		} else if typ == HASH_ELE {
			if v, err := fe.Cmd("HGET", key, subkey).Str(); err != nil {
				log.Fatalf("hget addr2:%s:%s failed:%v", key, subkey, err)
			} else if v != val {
				log.Errorf("hash key:%s, subkey:%s, addr2:%s, back:%s", key, subkey, v, val)
			}
		} else if typ == SET_ELE {
			if v, err := fe.Cmd("SISMEMBER", key, subkey).Int64(); err != nil {
				log.Fatalf("set addr2:%s:%s failed:%v", key, subkey, err)
			} else if v != 1 {
				log.Errorf("set key:%s, subkey:%s, addr2:%d, back:1", key, subkey, v)
			}
		} else if typ == ZSET_H_ELE {
			score, _ := strconv.ParseFloat(val, 64)
			if v, err := fe.Cmd("ZSCORE", key, subkey).Float64(); err != nil {
				log.Fatalf("zset addr2:%s:%s failed:%v", key, subkey, err)
			} else if score != v {
				log.Errorf("zset key:%s, subkey:%s, addr2:%d, back:%d", key, subkey, v, score)
			}
		}
		cnt += 1
	}
	return cnt
}

func processOneStore(storeId int, addr1 string, addr2 string, channel chan int) {
	be, err := redis.DialTimeout("tcp", addr1, 20*time.Second)
	if err != nil {
		log.Fatalf("dial %s failed:%v", addr1, err)
	}
	if *password1 != "" {
		if v, err := be.Cmd("AUTH", *password1).Str(); err != nil || v != "OK" {
			log.Fatalf("auth %s failed", addr1)
		}
	}

	var jobs_channel chan []*redis.Resp = make(chan []*redis.Resp, 2000)
	var result []int
	var result_lock sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		go procCoroutinue(i, addr2, jobs_channel, &result, &result_lock, &wg)
	}

	batch := 1000
	cnt := 0

	iter := "0"
	for {
		if arr, err := be.Cmd("iterall", storeId, iter, batch).Array(); err != nil {
			log.Fatalf("iter store:%d failed:%v", storeId, err)
		} else {
			if err != nil {
				log.Fatalf("parse into arr failed:%v", err)
			}
			if len(arr) != 2 {
				log.Fatalf("invalid arr size:%v", len(arr))
			}
			if newIter, err := arr[0].Str(); err != nil {
				log.Fatalf("parse arr[0] to str failed:%v", err)
			} else {
				iter = newIter
			}
			arr1, err := arr[1].Array()
			if err != nil {
				log.Fatalf("parse into batch failed:%v", err)
			}

			wg.Add(1)
			jobs_channel <- arr1

			if iter == "0" {
				break
			}
		}
	}
	close(jobs_channel)

	wg.Wait()
	for _, ret := range result {
		cnt += ret
	}

	fmt.Printf("store %d compared %d records,b:%s f:%s, finish\n", storeId, cnt, addr1, addr2)
	channel <- cnt
}
