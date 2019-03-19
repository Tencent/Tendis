#ifndef SRC_TENDISPLUS_STORAGE_KVSTORE_H_
#define SRC_TENDISPLUS_STORAGE_KVSTORE_H_

#include <limits>
#include <string>
#include <utility>
#include <memory>
#include <iostream>
#include <list>
#include <map>
#include <vector>
#include <atomic>

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/storage/record.h"

#define CATALOG_NAME "catalog"

namespace tendisplus {

class KVStore;
class Record;
class ReplLog;
class TTLIndex;
class RecordKey;
class RecordValue;

using PStore = std::shared_ptr<KVStore>;

class Cursor {
 public:
    Cursor() = default;
    virtual ~Cursor() = default;
    virtual void seek(const std::string& prefix) = 0;
    // seek to last of the collection, Not the prefix
    virtual void seekToLast() = 0;
    virtual Expected<Record> next() = 0;
    virtual Status prev() = 0;
    virtual Expected<std::string> key() = 0;
};

class BinlogCursor {
 public:
    BinlogCursor() = delete;
    // NOTE(deyukong): in range of [begin, end], be careful both close interval
    BinlogCursor(std::unique_ptr<Cursor> cursor, uint64_t begin, uint64_t end);
    ~BinlogCursor() = default;
    Expected<ReplLog> next();
    void seekToLast();

 protected:
    std::unique_ptr<Cursor> _baseCursor;

 private:
    const std::string _beginPrefix;
    const uint64_t _end;
};

class TTLIndexCursor {
 public:
    TTLIndexCursor() = delete;
    TTLIndexCursor(std::unique_ptr<Cursor> cursor, std::uint64_t until);
    ~TTLIndexCursor() = default;
    Expected<TTLIndex> next();
    void prev();
    void seek(const std::string &target);
    Expected<std::string> key();

 private:
    const uint64_t _until;

 protected:
    std::unique_ptr<Cursor> _baseCursor;
};

class Transaction {
 public:
    Transaction() = default;
    Transaction(const Transaction&) = delete;
    Transaction(Transaction&&) = delete;
    virtual ~Transaction() = default;
    virtual Expected<uint64_t> commit() = 0;
    virtual Status rollback() = 0;
    virtual std::unique_ptr<Cursor> createCursor() = 0;
    virtual std::unique_ptr<BinlogCursor>
        createBinlogCursor(uint64_t begin, bool ignoreReadBarrier = false) = 0;
    virtual std::unique_ptr<TTLIndexCursor>
        createTTLIndexCursor(std::uint64_t until) = 0;
    virtual Expected<std::string> getKV(const std::string& key) = 0;
    virtual Status setKV(const std::string& key,
                         const std::string& val,
                         const uint32_t ts = 0) = 0;
    virtual Status delKV(const std::string& key, const uint32_t ts = 0) = 0;
    virtual Status applyBinlog(const std::list<ReplLog>& txnLog) = 0;
    virtual Status truncateBinlog(const std::list<ReplLog>& txnLog) = 0;

    virtual uint32_t getBinlogTime() = 0;
    virtual void setBinlogTime(uint32_t timestamp) = 0;

    static constexpr uint64_t MAX_VALID_TXNID
        = std::numeric_limits<uint64_t>::max()/2;
    static constexpr uint64_t MIN_VALID_TXNID = 1;
    static constexpr uint64_t TXNID_UNINITED = 0;
};

class BackupInfo {
 public:
    BackupInfo();
    const std::map<std::string, uint64_t>& getFileList() const;
    void setFileList(const std::map<std::string, uint64_t>&);
    void setBinlogPos(uint64_t);
    uint64_t getBinlogPos() const;
 private:
    std::map<std::string, uint64_t> _fileList;
    uint64_t _binlogPos;
};

class BinlogObserver {
 public:
    virtual void onCommit(const std::vector<ReplLog>& binlogs) = 0;
    virtual ~BinlogObserver() = default;
};

class KVStore {
 public:
    enum class StoreMode {
        READ_WRITE,
        REPLICATE_ONLY,
    };

    enum class BackupMode {
        BACKUP_COPY,
        BACKUP_CKPT,
    };
    explicit KVStore(const std::string& id, const std::string& path);
    virtual ~KVStore() = default;
    const std::string& dbPath() const { return _dbPath; }
    const std::string& dbId() const { return _id; }
    const std::string dftBackupDir() const { return _backupDir; }
    virtual Expected<std::unique_ptr<Transaction>> createTransaction() = 0;
    virtual Expected<RecordValue> getKV(const RecordKey& key,
                                        Transaction* txn) = 0;
    virtual Status setKV(const RecordKey&,
                         const RecordValue&, Transaction*) = 0;
    virtual Status setKV(const Record& kv, Transaction* txn) = 0;
    // TODO(eliotwang) deprecate this member function
    virtual Status setKV(const std::string& key, const std::string& val,
                         Transaction* txn) = 0;
    virtual Status delKV(const RecordKey& key, Transaction* txn) = 0;
    virtual Status applyBinlog(const std::list<ReplLog>& txnLog,
                               Transaction* txn) = 0;

    // get binlogs in [start, end], and check if start == "the first binlog"
    // return the "next first binlog" and a list of binlogs to be deleted.
    // if no visible binlogs, "next first binlog" == start
    virtual Expected<std::pair<uint64_t, std::list<ReplLog>>> getTruncateLog(
            uint64_t start, uint64_t end, Transaction* txn) = 0;

    virtual Status truncateBinlog(const std::list<ReplLog>&, Transaction*) = 0;

    virtual Status setLogObserver(std::shared_ptr<BinlogObserver>) = 0;

    // remove all data in db
    virtual Status clear() = 0;

    virtual bool isRunning() const = 0;
    virtual Status stop() = 0;

    virtual Status setMode(StoreMode mode) = 0;
    virtual KVStore::StoreMode getMode() = 0;

    // return the greatest commitId
    virtual Expected<uint64_t> restart(bool restore = false) = 0;

    // backup related apis, allows only one backup at a time
    // backup and return the filename<->filesize pair
    virtual Expected<BackupInfo> backup(const std::string&, BackupMode) = 0;
    virtual Status releaseBackup() = 0;

    virtual void appendJSONStat(
        rapidjson::Writer<rapidjson::StringBuffer>&) const = 0;

    uint32_t getBinlogTime();
    void setBinlogTime(uint32_t timestamp);
    uint32_t getCurrentTime();

    // NOTE(deyukong): INSTANCE_NUM can not be dynamicly changed.
    static constexpr size_t INSTANCE_NUM = size_t(10);
    

 private:
    const std::string _id;
    const std::string _dbPath;
    const std::string _backupDir;
    std::atomic<uint32_t> _binlogTimeSpov;
};

}  // namespace tendisplus

#endif   // SRC_TENDISPLUS_STORAGE_KVSTORE_H_
