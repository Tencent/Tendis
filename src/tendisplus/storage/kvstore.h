#ifndef SRC_TENDISPLUS_STORAGE_KVSTORE_H_
#define SRC_TENDISPLUS_STORAGE_KVSTORE_H_

#include <limits>
#include <string>
#include <utility>
#include <memory>
#include <iostream>
#include <map>
#include "tendisplus/utils/status.h"
#include "tendisplus/storage/record.h"

namespace tendisplus {

class KVStore;
class Record;
class ReplLog;
class RecordKey;
class RecordValue;

using PStore = std::shared_ptr<KVStore>;

class Cursor {
 public:
    Cursor() = default;
    virtual ~Cursor() = default;
    virtual void seek(const std::string& prefix) = 0;
    virtual Expected<Record> next() = 0;
};

class BinlogCursor {
 public:
    BinlogCursor() = delete;
    BinlogCursor(std::unique_ptr<Cursor> cursor, uint64_t begin, uint64_t end);
    ~BinlogCursor() = default;
    // NOTE(deyukong): in range of [begin, end), be careful right is open
    Expected<ReplLog> next();

 protected:
    std::unique_ptr<Cursor> _baseCursor;

 private:
    const std::string _beginPrefix;
    const uint64_t _end;
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
        createBinlogCursor(uint64_t begin) = 0;
    virtual Expected<std::string> getKV(const std::string& key) = 0;
    virtual Status setKV(const std::string& key,
                         const std::string& val,
                         bool withLog) = 0;
    virtual Status delKV(const std::string& key, bool withLog) = 0;
    static constexpr uint64_t MAX_VALID_TXNID
        = std::numeric_limits<uint64_t>::max()/2;
    static constexpr uint64_t MIN_VALID_TXNID = 0;
    static constexpr uint64_t TXNID_UNINITED
        = std::numeric_limits<uint64_t>::max()/2+1;
};

class BackupInfo {
 public:
    BackupInfo() = default;
    const std::map<std::string, uint64_t>& getFileList() const;
    void setFileList(const std::map<std::string, uint64_t>&);
 private:
    std::map<std::string, uint64_t> _fileList;
};

class KVStore {
 public:
    explicit KVStore(const std::string& id, const std::string& path);
    virtual ~KVStore() = default;
    const std::string& dbPath() const { return _dbPath; }
    const std::string& dbId() const { return _id; }
    const std::string backupDir() const { return _backupDir; }
    virtual Expected<std::unique_ptr<Transaction>> createTransaction() = 0;
    virtual Expected<RecordValue> getKV(const RecordKey& key,
        Transaction* txn) = 0;
    virtual Status setKV(const RecordKey&,
        const RecordValue&, Transaction*, bool withLog = true) = 0;
    virtual Status setKV(const Record& kv, Transaction* txn,
        bool withLog = true) = 0;
    virtual Status setKV(const std::string& key, const std::string& val,
        Transaction* txn, bool withLog = true) = 0;
    virtual Status delKV(const RecordKey& key, Transaction* txn,
        bool withLog = true) = 0;

    // remove all data in db
    virtual Status clear() = 0;
    virtual bool isRunning() const = 0;
    virtual Status stop() = 0;

    // return the greatest commitId
    virtual Expected<uint64_t> restart(bool restore = false) = 0;

    // backup related apis, allows only one backup at a time
    // backup and return the filename<->filesize pair
    virtual Expected<BackupInfo> backup() = 0;
    virtual Status releaseBackup() = 0;

    // NOTE(deyukong): INSTANCE_NUM can not be dynamicly changed.
    static constexpr size_t INSTANCE_NUM = size_t(100);

 private:
    const std::string _id;
    const std::string _dbPath;
    const std::string _backupDir;
};

}  // namespace tendisplus

#endif   // SRC_TENDISPLUS_STORAGE_KVSTORE_H_
