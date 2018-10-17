package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/ngaut/log"
	"time"
)

var (
	ip     = flag.String("host", "127.0.0.1", "server hostname")
	port   = flag.Uint("port", 6379, "server port")
	passwd = flag.String("password", "", "server password")
)

type DebugInfo struct {
	Stores map[string] struct {
		IsRunning   int    `json:"is_running"`
		HasBackup   int    `json:"has_backup"`
		NextTxnSeq  uint64 `json:"next_txn_seq"`
		AliveTxns   uint64 `json:"alive_txns"`
		MinAliveTxn uint64 `json:"min_alive_txn"`
		MaxAliveTxn uint64 `json:"max_alive_txn"`
		HighVisible uint64 `json:"high_visible"`
	} `json:"stores"`
	Repl map[string] struct {
		SyncDest map[string] struct {
			IsRunning   int    `json:"is_running"`
			DestStoreId int    `json:"dest_store_id"`
			BinlogPos   uint64 `json:"binlog_pos"`
			RemoteHost  string `json:"remote_host"`
		} `json:"sync_dest"`
	} `json:"repl"`
	UnseenCommands map[string]uint64 `json:"unseen_commands"`
	Commands       map[string] struct {
		CallTimes  uint64 `json:"call_times"`
		TotalNanos uint64 `json:"total_nanos"`
	} `json:"commands"`
}

func main() {
	flag.Parse()
	host := fmt.Sprintf("%s:%d", *ip, *port)
	client, err := redis.DialTimeout("tcp", host, 1*time.Second)
	if err != nil {
		log.Fatalf("dial host %s failed:%v", host, err)
	}
	v, err := client.Cmd("DEBUG").Str()
	if err != nil {
		log.Fatalf("send debug cmd failed:%v", err)
	}
	info := DebugInfo{}
	if err := json.Unmarshal([]byte(v), &info); err != nil {
		log.Fatalf("unmarshal debug info failed:%v", err)
	}
	log.Infof("%+v", info)
}
