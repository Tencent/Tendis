package util

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/ngaut/log"
	"github.com/redis/go-redis/v9"
)

var (
	// NOTE(barneyxiao): Optype support set,lpush,sadd,hset,zadd.
	Optype = flag.String("optype", "set", "add data operation type")
)

func CreateClientWithGoRedis(m *RedisServer, auth string) *redis.UniversalClient {
	var rdb redis.UniversalClient
	switch m.ClientType {
	case Standalone:
		rdb = redis.NewClient(&redis.Options{
			Addr:     m.Ip + ":" + strconv.Itoa(m.Port),
			Password: auth,
		})
	case Cluster:
		rdb = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    []string{m.Ip + ":" + strconv.Itoa(m.Port)},
			Password: auth,
		})
	}
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("create client for add data failed. port:%d. err:%v", m.Port, err)
	}
	return &rdb
}

func GenerateKey(optype string, number int, prefixkey string) string {
	return optype + "_key_" + prefixkey + "_" + strconv.Itoa(number)
}
func GenerateKeyForBenchmark(prefixkey string, number int) string {
	return fmt.Sprintf("key:%04s%010d", prefixkey, number)
}
func AddTypeDataWithNum(cli *redis.UniversalClient, optype string, startNo int, keyNumber int, fieldNumber int, expiredMs int, valueLen int, prefixkey string) {
	supportOpt := map[string]string{
		"set": "set", "lpush": "lpush", "hset": "hset", "sadd": "sadd", "zadd": "zadd", "hmset": "hmset",
	}
	if _, ok := supportOpt[optype]; !ok {
		log.Fatalf("unsupport optype : %v", optype)
	}
	ctx := context.Background()
	var key string
	retryTime := 3
	for i := startNo; i < startNo+keyNumber; i++ {
		var args []interface{}
		args = append(args, supportOpt[optype])
		key = GenerateKey(optype, i, prefixkey)
		args = append(args, key)

		if optype == "set" {
			args = append(args, "value"+"_"+strconv.Itoa(i)+"_"+RandStrAlpha(valueLen))
		} else {
			for j := 0; j < fieldNumber; j++ {
				if optype == "sadd" || optype == "lpush" {
					args = append(args, "field_"+strconv.Itoa(j)+"_"+RandStrAlpha(valueLen))
				} else if optype == "hset" || optype == "hmset" {
					args = append(args, "field_"+strconv.Itoa(j), "value_"+strconv.Itoa(j)+"_"+RandStrAlpha(valueLen))
				} else if optype == "zadd" {
					args = append(args, strconv.Itoa(i), "field_"+strconv.Itoa(j)+"_"+RandStrAlpha(valueLen))
				}
			}
		}
		for i := 0; i < retryTime; i++ {
			if err := (*cli).Do(ctx, args...).Err(); err != nil {
				log.Warningf("insert data failed, optype:%v, err:%v", optype, err)
				time.Sleep(1 * time.Second)
			} else {
				break
			}
			if i == retryTime-1 {
				log.Fatalf("insert data failed, optype:%v", optype)
			}
		}

		if expiredMs > 0 {
			if err := (*cli).PExpire(ctx, key, time.Millisecond*time.Duration(expiredMs)).Err(); err != nil {
				log.Fatalf("insert data expire failed. %v", err)
			}
		}
	}
}

func InsertAndExpiredHashData(m *RedisServer, auth string, startNo int, fieldNumber int, expiredMs int, keyPrefix string) {
	cli := CreateClientWithGoRedis(m, auth)
	ctx := context.Background()
	AddTypeDataWithNum(cli, "hset", startNo, 1, fieldNumber, expiredMs, 30, keyPrefix)
	key := GenerateKey(*Optype, startNo, keyPrefix)
	if rand.Intn(10) < 7 {
		if err := (*cli).Del(ctx, key).Err(); err != nil {
			log.Fatalf("del data failed. %v", err)
		}
	}
}

func AddSomeData(m *RedisServer, auth string, startNo int) {
	log.Infof("m.ip:%v, m.port:%v", m.Ip, m.Port)
	cli := CreateClientWithGoRedis(m, auth)

	AddTypeDataWithNum(cli, "set", startNo, 100, 0, 0, 30, "")
	AddTypeDataWithNum(cli, "zadd", startNo, 100, 3, 0, 30, "")
	AddTypeDataWithNum(cli, "sadd", startNo, 100, 3, 0, 30, "")
	AddTypeDataWithNum(cli, "lpush", startNo, 200, 3, 0, 30, "")
	AddTypeDataWithNum(cli, "hset", startNo, 100, 4, 0, 30, "")
	AddTypeDataWithNum(cli, "hset", startNo+200, 1, 10000, 0, 20, "")
	AddTypeDataWithNum(cli, "set", startNo+300, 10000, 0, rand.Intn(1000)+1, 20, "")
	log.Infof("add some data end. m.ip:%v, m.port:%v", m.Ip, m.Port)
}

func AddSingleData(cli *redis.UniversalClient, startNo int, expiredMs int, valueLen int, keyPrefix string) {
	AddTypeDataWithNum(cli, "set", startNo, 1, 0, expiredMs, valueLen, keyPrefix)
	AddTypeDataWithNum(cli, "lpush", startNo, 1, 1, expiredMs, valueLen, keyPrefix)
	AddTypeDataWithNum(cli, "hset", startNo, 1, 1, expiredMs, valueLen, keyPrefix)
	AddTypeDataWithNum(cli, "sadd", startNo, 1, 1, expiredMs, valueLen, keyPrefix)
	AddTypeDataWithNum(cli, "zadd", startNo, 1, 1, expiredMs, valueLen, keyPrefix)
}

func AddData(m *RedisServer, auth string, keyNumber int, expiredMs int, valueLen int, keyPrefix string, ch chan int) {
	log.Infof("m.ip:%v, m.port:%v", m.Ip, m.Port)
	go func() {
		cli := CreateClientWithGoRedis(m, auth)
		for i := 0; i < keyNumber; i++ {
			AddSingleData(cli, i, expiredMs, valueLen, keyPrefix)
		}
		ch <- keyNumber * 5
		log.Infof("add data end. m.ip:%v, m.port:%v", m.Ip, m.Port)
	}()
}

func AddDataWithTime(m *RedisServer, auth string, seconds int, expiredMs int, keyPrefix string, valueLen int, ch chan int) {
	log.Infof("add data with time begin m.IP:%v, m.Port:%v", m.Ip, m.Port)
	go func() {
		cli := CreateClientWithGoRedis(m, auth)
		i := 0
		deadLine := time.Now().Add(time.Duration(seconds) * time.Second)
		for {
			AddSingleData(cli, i, expiredMs, valueLen, keyPrefix)
			i++
			time.Sleep(500 * time.Microsecond)

			if time.Now().After(deadLine) {
				break
			}
		}
		log.Info("add data with time ok.")
		ch <- i * 5
	}()
}

func AddDataWithBenchmark(m *RedisServer, auth string, num int, prefixkey string, benchType string) {
	log.Infof("addData begin. %s:%d", m.Ip, m.Port)
	maxWaitTimeout := 1000 * time.Second
	logFilePath := fmt.Sprintf("benchmark_%d.log", m.Port)
	var cmd string
	cmd = fmt.Sprintf("../../../bin/redis-benchmark -h %s -p %d -c 20 -n %d -r 8 -i -f %s -t %s -a %s -e > %s 2>&1",
		m.Ip, m.Port, num, prefixkey, benchType, auth, logFilePath)
	log.Infof("addData cmd:%s", cmd)
	args := []string{}
	args = append(args, cmd)
	inShell := true
	_, err := StartProcess(args, []string{}, "", maxWaitTimeout, inShell, nil)
	if err != nil {
		log.Fatalf("addData failed:%v", err)
		return
	}

	log.Infof("addData sucess. %s:%d num:%d", m.Ip, m.Port, num)
}

func AddDataWithBenchmarkInCo(m *RedisServer, auth string, num int, prefixkey string, benchType string, channel chan int) {
	AddDataWithBenchmark(m, auth, num, prefixkey, benchType)
	channel <- num
}

func AddDataInCoroutine(m *RedisServer, auth string, num int, prefixkey string, channel chan int, optype string) {
	cli := CreateClientWithGoRedis(m, auth)
	log.Infof("optype is : %s", optype)
	AddTypeDataWithNum(cli, optype, 0, num, 1, 0, 20, prefixkey)
	channel <- num
}
func AddOnekeyEveryStore(m *RedisServer, auth string, kvstorecount int) {
	cli := CreateClientWithGoRedis(m, auth)
	ctx := context.Background()
	for i := 0; i < kvstorecount; i++ {
		if _, err := (*cli).Do(ctx, "setinstore", strconv.Itoa(i), "fixed_test_key", "fixed_test_value").Result(); err != nil {
			log.Fatalf("do addOnekeyEveryStore %d failed:%v", i, err)
		}
	}
	log.Info("addOnekeyEveryStore sucess")
}

func ExpireKey(m *RedisServer, auth string, num int, prefixkey string, keyFormatType string) {
	log.Info("expireKey begin")
	cli := CreateClientWithGoRedis(m, auth)
	ctx := context.Background()
	var key string
	for i := 1; i <= num; i++ {
		if keyFormatType == "benchmark" {
			key = GenerateKeyForBenchmark(prefixkey, i)
		} else {
			key = GenerateKey(*Optype, i, prefixkey)
		}
		if val, err := (*cli).PExpire(ctx, key, 120*time.Second).Result(); err != nil {
			log.Infof("expired failed, key:%v data:%v err:%v", key, val, err)
		}
	}
}

/*

	Check Data part

*/

func CheckTypeData(cli *redis.UniversalClient, optype string, startNo int, keyNumber int, fieldNumber int, prefixkey string) int {
	log.Infof("checkData begin(%v). num:%v, prefixkey:%v, startNo:%v", optype, keyNumber, prefixkey, startNo)

	// NOTE(barneyxiao): Compare specific value only when type is "set" and "redis-benchmark", other types compare fieldNumber
	type2op := map[string]string{
		"set": "get", "redis-benchmark": "get", "sadd": "scard", "hset": "hlen", "lpush": "llen", "zadd": "zcard",
	}
	if _, ok := type2op[optype]; !ok {
		log.Fatalf("unsupport optype : %v", optype)
	}
	ctx := context.Background()
	var key string
	wrongKeyNumber := 0
	for i := startNo; i < startNo+keyNumber; i++ {
		var args []interface{}
		args = append(args, type2op[optype])
		key = GenerateKey(optype, i, prefixkey)
		if optype == "redis-benchmark" {
			key = GenerateKeyForBenchmark(prefixkey, i)
		}
		args = append(args, key)
		ret, err := (*cli).Do(ctx, args...).Result()
		if err != nil {
			log.Fatalf("check data failed, optype:%v, err:%v", optype, err)
		}
		var value string
		value, ok := ret.(string)
		if !ok {
			value = strconv.FormatInt(ret.(int64), 10)
		}

		if optype == "set" {
			value = "value_" + strconv.Itoa(i) + "_"
		} else if optype == "redis-benchmark" {
			value = "xxx"
		} else {
			value = strconv.Itoa(fieldNumber)
		}
		if ret != value {
			wrongKeyNumber++
			log.Infof("find failed, key:%v, data:%v, value:%v, optype:%v", key, ret, value, optype)
		}
	}
	log.Infof("checkData End(%v). num:%v, prefixkey:%v, startNo:%v", optype, keyNumber, prefixkey, startNo)
	return wrongKeyNumber
}

func CheckDataInCoroutine(m *RedisServer, auth string, optype string, startNo int, keyNumber int, fieldNumber int, prefixkey string, channel chan int) {
	cli := CreateClientWithGoRedis(m, auth)
	channel <- CheckTypeData(cli, optype, startNo, keyNumber, fieldNumber, prefixkey)
}
