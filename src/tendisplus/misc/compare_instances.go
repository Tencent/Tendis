package main

import (
	"flag"
	"fmt"
	"os"
    "time"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/ngaut/log"
    "strconv"
    "sync"
)

const (
	KV         = 2
	LIST_ELE   = 4
	HASH_ELE   = 6
	SET_ELE    = 8
	ZSET_H_ELE = 11
)

var (
	// backend     = flag.String("backendhost", "9.24.0.133:10001", "backend tendisplus host")
	backendpass = flag.String("backendpassword", "", "backend password")
	frontproxy  = flag.String("frontproxy", "9.24.0.133:10001", "frontend proxy host")
	frontpass   = flag.String("frontpass", "", "front password")
)

func main() {
	flag.Parse()
	args := os.Args
	backend := args[1]
	frontend := args[2]
    var channel chan int = make(chan int)
    for i := 0; i < 10; i++ {
        go processOneStore(i, backend, frontend, channel)
    }
    var total int = 0
    for i := 0; i < 10; i++ {
        num := <- channel
        total += num
    }
	fmt.Printf("total %d records compared.b:%s f:%s\n", total, backend, frontend)
}

func procCoroutinue(procid int, frontend string, jobs <-chan []*redis.Resp,
    result *[]int, result_lock *sync.Mutex, wg *sync.WaitGroup) {
	fe, err := redis.DialTimeout("tcp", frontend, 10*time.Second)
	// fe, err := redis.DialTimeout("tcp", *frontproxy, 10*time.Second)
	if err != nil {
		log.Fatalf("dial %s failed:%v", *frontproxy, err)
	}
    if *frontpass != "" {
        if v, err := fe.Cmd("AUTH", *frontpass).Str(); err != nil || v != "OK" {
            log.Fatalf("auth %s failed", *frontproxy)
        }
    }

    for job := range jobs{
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
				log.Fatalf("get front:%s failed:%v", key, err)
			} else if v != val {
				log.Errorf("key:%s, front:%s, back:%s", key, v, val)
			}
		} else if typ == LIST_ELE {
			if v, err := fe.Cmd("LINDEX", key, subkey).Str(); err != nil {
			    log.Fatalf("lindex front:%s:%s failed:%v", key, subkey, err)
			} else if v != val {
			    log.Errorf("list key:%s, index:%s, front:%s, back:%s", key, subkey, v, val)
			}
 		} else if typ == HASH_ELE {
			if v, err := fe.Cmd("HGET", key, subkey).Str(); err != nil {
				log.Fatalf("hget front:%s:%s failed:%v", key, subkey, err)
			} else if v != val {
				log.Errorf("hash key:%s, subkey:%s, front:%s, back:%s", key, subkey, v, val)
			}
		} else if typ == SET_ELE {
			if v, err := fe.Cmd("SISMEMBER", key, subkey).Int64(); err != nil {
				log.Fatalf("set front:%s:%s failed:%v", key, subkey, err)
			} else if v != 1 {
				log.Errorf("set key:%s, subkey:%s, front:%d, back:1", key, subkey, v)
			}
		} else if typ == ZSET_H_ELE {
			score, _ := strconv.ParseFloat(val, 64)
			if v, err := fe.Cmd("ZSCORE", key, subkey).Float64(); err != nil {
				log.Fatalf("zset front:%s:%s failed:%v", key, subkey, err)
			} else if score != v {
				log.Errorf("zset key:%s, subkey:%s, front:%d, back:%d", key, subkey, v, score)
			}
		}
		cnt += 1
	}
    return cnt
}

func processOneStore(storeId int, backend string, frontend string, channel chan int) {
	be, err := redis.DialTimeout("tcp", backend, 10*time.Second)
	// be, err := redis.DialTimeout("tcp", *backend, 10*time.Second)
	if err != nil {
		log.Fatalf("dial %s failed:%v", backend, err)
	}
    if *backendpass != "" {
        if v, err := be.Cmd("AUTH", *backendpass).Str(); err != nil || v != "OK" {
            log.Fatalf("auth %s failed", backend)
        }
    }

    var jobs_channel chan []*redis.Resp = make(chan []*redis.Resp, 2000);
    var result []int;
    var result_lock sync.Mutex
    var wg sync.WaitGroup

    for i := 0; i < 100; i++{
        go procCoroutinue(i, frontend, jobs_channel, &result, &result_lock, &wg)
    }


	batch := 1000
	cnt := 0

	iter := "0"
	for {
		// fmt.Printf("iterall %d %s %d\n", storeId, iter, batch);
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

	fmt.Printf("store %d compared %d records,b:%s f:%s, finish\n", storeId, cnt, backend, frontend)
    channel <- cnt
}
