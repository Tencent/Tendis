#ifndef SRC_TENDISPLUS_STORAGE_KVSTORE_H_
#define SRC_TENDISPLUS_STORAGE_KVSTORE_H_

#include <string>
#include <utility>
#include <memory>
#include "tendisplus/utils/status.h"

namespace tendisplus {

class Transaction {
 public:
    Transaction() = default;
    Transaction(const Transaction&) = delete;
    Transaction(Transaction&&) = delete;
    virtual Status commit() = 0;
    virtual Status rollback() = 0;
    virtual Expected<std::string> getKV(const std::string& key) = 0;
    virtual Status setKV(const std::string& key, const std::string& val) = 0;
    virtual Status delKV(const std::string& key) = 0;
};

class KVStore {
 public:
    explicit KVStore(const std::string& id);
    virtual Expected<std::unique_ptr<Transaction>> createTransaction() = 0;
    virtual Expected<std::string> getKV(const std::string& key,
        Transaction* txn) = 0;
    virtual Status setKV(const std::string& key, const std::string& val,
        Transaction* txn) = 0;
    virtual Status delKV(const std::string& key, Transaction* txn) = 0;
 private:
    std::string _id;
};

}  // namespace tendisplus

#endif   // SRC_TENDISPLUS_STORAGE_KVSTORE_H_
