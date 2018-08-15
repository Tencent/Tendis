#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction.h"

#include <unistd.h>
#include <thread>
#include <vector>
#include <utility>
#include <memory>
#include <atomic>
#include <random>
#include <sstream>
#include <iostream>

using PATOM = std::shared_ptr<std::atomic<uint64_t>>;
using PDB = std::shared_ptr<rocksdb::OptimisticTransactionDB>;

uint32_t intRand() {
    static std::atomic<int> seed;
    static thread_local std::mt19937 generator(seed.fetch_add(1,std::memory_order_relaxed));
    uint32_t min = std::numeric_limits<uint32_t>::min();
    uint32_t max = std::numeric_limits<uint32_t>::max();
    std::uniform_int_distribution<uint32_t> distribution(min,max);
    return distribution(generator);
}

void run1(PDB txn_db, uint32_t nkeys, int idx, PATOM succ, PATOM confl) {
    while (true) {
        rocksdb::WriteOptions write_options;
        uint32_t randnum = intRand() % nkeys;
        std::stringstream ss;
        ss << randnum;
        //std::cout<<ss.str() << std::endl;
        txn_db->GetBaseDB()->Put(write_options, ss.str(), "aaaaa");
        (*succ)++;
    }
}

void run(PDB txn_db, uint32_t nkeys, int idx, PATOM succ, PATOM confl) {
    while (true) {
        rocksdb::WriteOptions write_options;
        rocksdb::Transaction* txn = txn_db->BeginTransaction(write_options);
        uint32_t randnum = intRand() % nkeys;
        std::stringstream ss;
        ss << randnum;
        //std::cout<<ss.str() << std::endl;
        txn->Put(ss.str(), "aaaaa");
        auto s = txn->Commit();
        if (s.ok()) {
            (*succ)++;
        } else {
            (*confl)++;
        }
        delete txn;
    }
}

void usage() {
    std::cout<< "./bin thread_num key_num" << std::endl;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        usage();
        return 0;
    }
    size_t thread_num = 0;
    size_t key_num = 0;
    std::stringstream ss(argv[1]), ss1(argv[2]);
    ss >> thread_num;
    ss1 >> key_num;
    rocksdb::OptimisticTransactionDB* txn_db = nullptr;
    rocksdb::Options options;
    options.write_buffer_size = 64*1024*1024;  // 64MB
    options.level0_slowdown_writes_trigger = 8;
    options.max_write_buffer_number = 4;
    options.max_background_compactions = 8;
    options.max_background_flushes = 2;
    options.target_file_size_base = 64 * 1024 * 1024; // 64MB
    options.soft_rate_limit = 2.5;
    options.hard_rate_limit = 3;
    options.level_compaction_dynamic_level_bytes = true;
    options.max_bytes_for_level_base = 512 * 1024 * 1024;  // 512 MB
    // This means there is no limit on open files. Make sure to always set ulimit so that it can
    // keep all RocksDB files opened.
    options.max_open_files = -1;
    options.enable_thread_tracking = true;
    options.enable_write_thread_adaptive_yield = false;
    options.create_if_missing = true;
    const std::string dbname = "./test";
    rocksdb::Status s = rocksdb::OptimisticTransactionDB::Open(options, dbname, &txn_db);
    assert(s.ok());
    PDB pdb(txn_db);
    std::vector<std::thread> threads;
    auto succ_updates = std::make_shared<std::atomic<uint64_t>>(0);
    auto conflict_updates = std::make_shared<std::atomic<uint64_t>>(0);
    for (int i = 0; i < thread_num; i++) {
        threads.emplace_back(std::thread(run1, std::ref(pdb), key_num,
            i, std::ref(succ_updates), std::ref(conflict_updates)));
    }
    while (true) {
        sleep(1);
        std::cout<< "succ:" << *succ_updates << ",confl:" << *conflict_updates << std::endl;
        (*succ_updates).store(0);
        (*conflict_updates).store(0);
    }
    return 0;
}
