// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

package main

import (
	"encoding/binary"
	"flag"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/ngaut/log"
	"os"
    "time"
)

var (
	restoreFile = flag.String("restoreFile", "", "file to restore from")
	host        = flag.String("host", "", "host to restore to")
	storeId     = flag.Int("storeId", 0, "storeid to restore to")
)

type Binlog struct {
	Id  uint64
	Key []byte
	Val []byte
}

func readNextBytes(file *os.File, number uint32) []byte {
	bytes := make([]byte, number)

	_, err := file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}

	return bytes
}

func applyBinlogs(binlogs []*Binlog, c *redis.Client) {
    if len(binlogs) == 0 {
        return
    }
	args := []interface{}{}
	args = append(args, *storeId)
	for _, o := range binlogs {
		args = append(args, o.Key)
		args = append(args, o.Val)
	}
	if v, err := c.Cmd("restoreBinlog", args...).Str(); err != nil || v != "OK" {
		log.Fatalf("restoreLog failed:%v %s", err, v)
	}
}

func main() {
	flag.Parse()
	file, err := os.Open(*restoreFile)
	if err != nil {
		log.Fatal("Error while opening file", err)
	}
	defer file.Close()

	client, err := redis.DialTimeout("tcp", *host, 10*time.Second)
	if err != nil {
		log.Fatalf("dial %s failed:%v", *host, err)
	}

	fileSize := int64(0)
	if s, err := file.Stat(); err != nil {
		log.Fatal("file stat failed:%v", err)
	} else {
		fileSize = s.Size()
	}
	nowSize := int64(0)
	nowTxnId := uint64(0)
	nxtTxnId := uint64(0)
	logBatch := []*Binlog{}

	for nowSize < fileSize {
		bytes := readNextBytes(file, 8)
		nxtTxnId = binary.BigEndian.Uint64(bytes)
		if nxtTxnId != nowTxnId {
			applyBinlogs(logBatch, client)
			logBatch = []*Binlog{}
			nowTxnId = nxtTxnId
		}
		bytes = readNextBytes(file, 4)
		keyLen := binary.BigEndian.Uint32(bytes)
		key := readNextBytes(file, keyLen)

		bytes = readNextBytes(file, 4)
		valLen := binary.BigEndian.Uint32(bytes)
		val := readNextBytes(file, valLen)

		nowSize += 8 + 4 + 4 + int64(keyLen) + int64(valLen)
		logBatch = append(logBatch, &Binlog{
			Id:  nowTxnId,
			Key: key,
			Val: val,
		})
	}
	if len(logBatch) != 0 {
		applyBinlogs(logBatch, client)
	}
	if nowSize != fileSize {
		log.Fatal("invalid file end")
	}
	log.Infof("apply binlog complete")
}
