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
	Stores map[string]struct {
		IsRunning   int    `json:"is_running"`
		HasBackup   int    `json:"has_backup"`
		NextTxnSeq  uint64 `json:"next_txn_seq"`
		AliveTxns   uint64 `json:"alive_txns"`
		MinAliveTxn uint64 `json:"min_alive_txn"`
		MaxAliveTxn uint64 `json:"max_alive_txn"`
		HighVisible uint64 `json:"high_visible"`
		Rocksdb     struct {
			NumImmutableMemTable           uint64 `json:"num_immutable_mem_table"`
			MemTableFlushPending           uint64 `json:"mem_table_flush_pending"`
			CompactionPending              uint64 `json:"compaction_pending"`
			BackgroundErrors               uint64 `json:"background_errors"`
			CurSizeActiveMemTable          uint64 `json:"cur_size_active_mem_table"`
			CurSizeAllMemTables            uint64 `json:"cur_size_all_mem_tables"`
			SizeAllMemTables               uint64 `json:"size_all_mem_tables"`
			NumEntriesActiveMemTable       uint64 `json:"num_entries_active_mem_table"`
			NumEntriesImmMemTables         uint64 `json:"num_entries_imm_mem_tables"`
			NumDeletesActiveMemTable       uint64 `json:"num_deletes_active_mem_table"`
			NumDeletesImmMemTables         uint64 `json:"num_deletes_imm_mem_tables"`
			EstimateNumKeys                uint64 `json:"estimate_num_keys"`
			EstimateTableReadersMem        uint64 `json:"estimate_table_readers_mem"`
			IsFileDeletionsEnabled         uint64 `json:"is_file_deletions_enabled"`
			NumSnapshots                   uint64 `json:"num_snapshots"`
			OldestSnapshotTime             uint64 `json:"oldest_snapshot_time"`
			NumLiveVersions                uint64 `json:"num_live_versions"`
			CurrentSuperVersionNumber      uint64 `json:"current_super_version_number"`
			EstimateLiveDataSize           uint64 `json:"estimate_live_data_size"`
			MinLogNumberToKeep             uint64 `json:"min_log_number_to_keep"`
			TotalSstFilesSize              uint64 `json:"total_sst_files_size"`
			LiveSstFilesSize               uint64 `json:"live_sst_files_size"`
			BaseLevel                      uint64 `json:"base_level"`
			EstimatePendingCompactionBytes uint64 `json:"estimate_pending_compaction_bytes"`
			NumRunningCompactions          uint64 `json:"num_running_compactions"`
			NumRunningFlushes              uint64 `json:"num_running_flushses"`
			ActualDelayedWriteRate         uint64 `json:"actual_delayed_write_rate"`
			IsWriteStopped                 uint64 `json:"is_write_stopped"`
			EstimateOldestKeyTime          uint64 `json:"estimate_oldest_key_time"`
        } `json:"rocksdb"`
	} `json:"stores"`
	Repl map[string]struct {
		SyncDest map[string]struct {
			IsRunning   int    `json:"is_running"`
			DestStoreId int    `json:"dest_store_id"`
			BinlogPos   uint64 `json:"binlog_pos"`
			RemoteHost  string `json:"remote_host"`
		} `json:"sync_dest"`
	} `json:"repl"`
	UnseenCommands map[string]uint64 `json:"unseen_commands"`
	Commands       map[string]struct {
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
