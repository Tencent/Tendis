package main

import (
    "math/rand"
    "github.com/mediocregopher/radix.v2/redis"
    "time"
    "github.com/ngaut/log"
    "flag"
    "fmt"
    "sync"
    "sync/atomic"
)

var threadNum = flag.Int("thd", 10, "thread num")
var host = flag.String("host", "127.0.0.1:6379", "host")
var run = flag.Int("run", 1, "load:0,run:1")
var gCounter = uint64(0)
var ten = uint64(0)
var fifty = uint64(0)
var hundred = uint64(0)

func randomString(len int) string {
    bytes := make([]byte, len)
    for i := 0; i < len; i++ {
        bytes[i] = byte(rand.Intn(255))
    }
    return string(bytes)
}

func dial(host string) (*redis.Client, error) {
    client, err := redis.DialTimeout("tcp", host, 10*time.Second)
    if err != nil {
        return nil, err
    }
    return client, nil
}

func runFunctor(begin, end int, wg *sync.WaitGroup) {
    defer wg.Done()
    c, err := dial(*host)
    if err != nil {
        log.Fatalf("dail:%s failed:%v", *host, err)
    }

    var str2send = randomString(2000)
    cnt := 0
    for i := begin; i <= end; i++ {
        now := time.Now()
        var r *redis.Resp = nil
        if (*run) == 0 {
            r = c.Cmd("SET", fmt.Sprintf("%d", i), str2send)
        } else {
            key := rand.Intn(end - begin) + begin
            r = c.Cmd("GET", fmt.Sprintf("%d", key))
        }
        delta := time.Now().Sub(now)
        mills := delta.Nanoseconds()/1000000
        if (mills >= 10) {
            atomic.AddUint64(&ten, 1)
        }
        if (mills >= 50) {
            atomic.AddUint64(&fifty, 1)
        }
        if (mills >= 100) {
            atomic.AddUint64(&hundred, 1)
        }
        cnt += 1
        if r.Err != nil {
            log.Errorf("exec failed:%v", r.Err)
        } else {
            atomic.AddUint64(&gCounter, 1)
        }
    }
}

func main() {
    flag.Parse()
    total := 100000000
    delta := total/(*threadNum)
    var wg sync.WaitGroup
    for i := 0; i < *threadNum; i+=1 {
        wg.Add(1)
        go runFunctor(i*delta, (i+1)*delta, &wg);
    }
    go func() {
        oriGcount := gCounter
        for {
            log.Infof("insert counter:%d,%d,%d,%d,%d", gCounter, ten, fifty, hundred, gCounter - oriGcount)
            if gCounter >= 100000000 {
                break
            }
            oriGcount = gCounter
            time.Sleep(1*time.Second)
        }
    } ()
    wg.Wait()
}
