package main

import (
    "math/rand"
    "github.com/mediocregopher/radix.v2/redis"
    "time"
    "github.com/ngaut/log"
    "flag"
    "fmt"
    "sync/atomic"
)

var threadNum = flag.Int("thd", 10, "thread num")
var host = flag.String("host", "127.0.0.1:6379", "host")
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

func run(i int) {
    c, err := dial(*host)
    if err != nil {
        log.Fatalf("dail:%s failed:%v", *host, err)
    }

    var str2send = randomString(500)
    cnt := 0
    for {
        now := time.Now()
        r := c.Cmd("SET", fmt.Sprintf("%d", 100000000*i+cnt), str2send)
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
            log.Errorf("set failed:%v", r.Err)
        } else {
            atomic.AddUint64(&gCounter, 1)
        }
    }
}

func main() {
    flag.Parse()
    for i := 0; i < *threadNum; i+=1 {
        go run(i);
    }
    for {
        log.Infof("insert counter:%d,%d,%d,%d", gCounter, ten, fifty, hundred)
        time.Sleep(1*time.Second)
    }
}
