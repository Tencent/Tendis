package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/ngaut/log"
	"os"
	"text/tabwriter"
	"time"
)

var (
	ip     = flag.String("host", "127.0.0.1", "server hostname")
	port   = flag.Uint("port", 6379, "server port")
	passwd = flag.String("password", "", "server password")
)

type DebugInfo struct {
	Network struct {
		StickyPackets  uint64 `json:"sticky_packets"`
		ConnCreated    uint64 `json:"conn_created"`
		ConnReleased   uint64 `json:"conn_released"`
		InvalidPackets uint64 `json:"invalid_packets"`
	} `json:"network"`
	ReqPool struct {
		InQueue     uint64 `json:"in_queue"`
		Executed    uint64 `json:"executed"`
		QueueTime   uint64 `json:"queue_time"`
		ExecuteTime uint64 `json:"execute_time"`
	} `json:"req_pool"`
	Request struct {
		Processed      uint64 `json:"processed"`
		ReadPacketCost uint64 `json:"read_packet_cost"`
		ProcessCost    uint64 `json:"process_cost"`
		SendPacketCost uint64 `json:"send_packet_cost"`
	} `json:"request"`
	Stores map[string]struct {
		Id          string `json:"id"`
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
		} `json:"rocksdb"`
	} `json:"stores"`
	Repl map[string]struct {
		SyncDest map[string]struct {
			IsRunning   int    `json:"is_running"`
			DestStoreId int    `json:"dest_store_id"`
			BinlogPos   uint64 `json:"binlog_pos"`
			RemoteHost  string `json:"remote_host"`
		} `json:"sync_dest"`
		SyncSource   string `json:"sync_source"`
		BinlogId     uint64 `json:"binlog_id"`
		ReplState    uint64 `json:"repl_state"`
		LastSyncTime string `json:"last_sync_time"`
	} `json:"repl"`
	UnseenCommands map[string]uint64 `json:"unseen_commands"`
	Commands       map[string]struct {
		CallTimes  uint64 `json:"call_times"`
		TotalNanos uint64 `json:"total_nanos"`
	} `json:"commands"`
}

func normInt(val uint64, withB bool) string {
	if val < 1024 {
		if withB {
			return fmt.Sprintf("%dB", val)
		}
		return fmt.Sprintf("%d", val)
	}
	val = val / 1024
	if val < 1024 {
		return fmt.Sprintf("%dK", val)
	}
	val = val / 1024
	if val < 1024 {
		return fmt.Sprintf("%dM", val)
	}
	val = val / 1024
	return fmt.Sprintf("%dG", val)
}

func main() {
	flag.Parse()
	host := fmt.Sprintf("%s:%d", *ip, *port)
	client, err := redis.DialTimeout("tcp", host, 1*time.Second)
	if err != nil {
		log.Fatalf("dial host %s failed:%v", host, err)
	}
    if *passwd != "" {
        if v, err := client.Cmd("AUTH", *passwd).Str(); err != nil || v != "OK" {
            log.Fatalf("auth failed.")
        }
    }


	var oldInfo *DebugInfo = nil
	info := &DebugInfo{}
	w := tabwriter.NewWriter(os.Stdout,
		4,   // minwidth
		0,   // tabwidth
		0,   // padding
		' ', // padchar
		tabwriter.AlignRight|tabwriter.Debug)
	fmt.Fprintln(w, "at\timm\tfp\tcp\tbe\tsm\tetrm\tns\tqtps\tq\tlag\t")
	w.Flush()

	nextTime := time.Now().Add(1 * time.Second)
	for {
		v, err := client.Cmd("tendisstat").Str()
		if err != nil {
			log.Fatalf("send debug cmd failed:%v", err)
		}
		if err := json.Unmarshal([]byte(v), &info); err != nil {
			log.Fatalf("unmarshal debug info failed:%v", err)
		}
		if oldInfo == nil {
			oldInfo = info
			continue
		}
		activeTxns := uint64(0)
		nImms := uint64(0)
		flushPending := uint64(0)
		compactPending := uint64(0)
		backgroundErrs := uint64(0)
		sizeMemtable := uint64(0)
		estimem := uint64(0)
		numSnapshots := uint64(0)
		inqueue := info.ReqPool.InQueue
		qtps := info.Request.Processed - oldInfo.Request.Processed
		maxLag := int64(0)
		for _, v := range info.Stores {
			activeTxns += v.AliveTxns
			nImms += v.Rocksdb.NumImmutableMemTable
			flushPending += v.Rocksdb.MemTableFlushPending
			compactPending += v.Rocksdb.CompactionPending
			backgroundErrs += v.Rocksdb.BackgroundErrors
			sizeMemtable += v.Rocksdb.SizeAllMemTables
			estimem += v.Rocksdb.EstimateTableReadersMem
			numSnapshots += v.Rocksdb.NumSnapshots
			replList, ok := info.Repl[v.Id]
			if !ok {
				continue
			}
			for _, dst := range replList.SyncDest {
				if int64(v.HighVisible)-int64(dst.BinlogPos) > maxLag {
					maxLag = int64(v.HighVisible) - int64(dst.BinlogPos)
				}
			}
		}
		w.Init(os.Stdout,
			4,   // minwidth
			0,   // tabwidth
			0,   // padding
			' ', // padchar
			tabwriter.AlignRight|tabwriter.Debug)
		s := fmt.Sprintf("%d\t%d\t%d\t%d\t%d\t%s\t%s\t%d\t%s\t%d\t%d\t",
			activeTxns,
			nImms,
			flushPending,
			compactPending,
			backgroundErrs,
			normInt(sizeMemtable, true),
			normInt(estimem, true),
			numSnapshots,
			normInt(qtps, false),
			inqueue,
			maxLag)
		fmt.Fprintln(w, s)
		w.Flush()

		oldInfo = info
		info = &DebugInfo{}
		time.Sleep(nextTime.Sub(time.Now()))
		nextTime = time.Now().Add(1 * time.Second)
	}
}
