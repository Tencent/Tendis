// NOTE(deyukong): this file is in bad c++ style because it's pulled from elsewhere
// and it's only benchmark tool, doest not affect mainline. Donot pick any code
// from it.
#include <iostream>
#include <vector>
#include <random>
#include <sstream>
#include <memory>
#include <thread>
#include <assert.h>
#include <fstream>
#include <unistd.h>

#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/experimental.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/table.h"
#include "rocksdb/convenience.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction_db.h"

using DBS = std::vector<std::unique_ptr<rocksdb::DB>>;
using ODBS = std::vector<std::unique_ptr<rocksdb::OptimisticTransactionDB>>;
using PDBS = std::vector<std::unique_ptr<rocksdb::TransactionDB>>;
using CFS = std::vector<std::vector<std::unique_ptr<rocksdb::ColumnFamilyHandle>>>;

void usage() {
	std::cout<< "./bin [db/odb/pdb] [load/run] db_num cf_num thd_num memtable_num memtable_size record_num" << std::endl;
	std::cout<< "db_num: how many dbs you want to open" << std::endl;
	std::cout<< "cf_num: how many cfs you want to open each db" << std::endl;
    std::cout<< "db: default db" << std::endl;
    std::cout<< "odb: opt db" << std::endl;
    std::cout<< "pdb: pessim db" << std::endl;
}

rocksdb::ColumnFamilyOptions _cfOptions(int memtable_num, int memtable_size) {
	rocksdb::ColumnFamilyOptions options;

	rocksdb::BlockBasedTableOptions table_options;
	table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
	table_options.block_size = 16 * 1024; // 16KB
	table_options.format_version = 2;
	table_options.cache_index_and_filter_blocks = false;
	table_options.no_block_cache = true;
	options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

	options.write_buffer_size = memtable_size;  // 64MB
	options.level0_slowdown_writes_trigger = 8;
	options.max_write_buffer_number = memtable_num;
	options.target_file_size_base = 64 * 1024 * 1024; // 64MB
	options.soft_rate_limit = 2.5;
	options.hard_rate_limit = 3;
	options.level_compaction_dynamic_level_bytes = true;
	options.max_bytes_for_level_base = 512 * 1024 * 1024;  // 512 MB
	options.optimize_filters_for_hits = false;
	options.compression_per_level.resize(7);
	options.compression_per_level[0] = rocksdb::kNoCompression;
	options.compression_per_level[1] = rocksdb::kNoCompression;
	options.compression_per_level[2] = rocksdb::kNoCompression;
	options.compression_per_level[3] = rocksdb::kNoCompression;
	options.compression_per_level[4] = rocksdb::kNoCompression;
	options.compression_per_level[5] = rocksdb::kNoCompression;
	options.compression_per_level[6] = rocksdb::kNoCompression;

	return options;
}

rocksdb::Options _options(std::shared_ptr<rocksdb::Cache> bc, int memtable_num, int memtable_size) {
	rocksdb::Options options;
	rocksdb::BlockBasedTableOptions table_options;
	table_options.block_cache = bc;
	table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
	table_options.block_size = 16 * 1024; // 16KB
	table_options.format_version = 2;
	// table_options.cache_index_and_filter_blocks = rocksGlobalOptions.cacheIndexAndFilterBlocks;
	options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
	options.write_buffer_size = memtable_size;  // 64MB
	options.level0_slowdown_writes_trigger = 8;
	options.max_write_buffer_number = memtable_num;
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
	options.optimize_filters_for_hits = false;
	options.enable_thread_tracking = true;
	options.compression_per_level.resize(7);
	options.compression_per_level[0] = rocksdb::kNoCompression;
	options.compression_per_level[1] = rocksdb::kNoCompression;
	options.compression_per_level[2] = rocksdb::kNoCompression;
	options.compression_per_level[3] = rocksdb::kNoCompression;
	options.compression_per_level[4] = rocksdb::kNoCompression;
	options.compression_per_level[5] = rocksdb::kNoCompression;
	options.compression_per_level[6] = rocksdb::kNoCompression;
	// options.enable_pipelined_write = true;
	options.enable_write_thread_adaptive_yield = false;
	// create the DB if it's not already present
	options.create_if_missing = true;
	options.max_total_wal_size = 4294967296; // 4GB

	return options;
}

uint32_t intRand() {
	static std::atomic<int> seed;
    static thread_local std::mt19937 generator(seed.fetch_add(1,std::memory_order_relaxed));
	uint32_t min = std::numeric_limits<uint32_t>::min();
	uint32_t max = std::numeric_limits<uint32_t>::max();
    std::uniform_int_distribution<uint32_t> distribution(min,max);
    return distribution(generator);
}

std::string genRandom(const size_t len) {
	return std::string(len, 'c');
}

/*
std::string genRandom(const size_t len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

	std::stringstream ss;
    for (size_t i = 0; i < len; ++i) {
		ss << alphanum[intRand() % sizeof(alphanum)];
    }
	return ss.str();
}
*/

void doPut(const DBS& dbs, const CFS& cfs) {
	uint32_t key = intRand();
	uint32_t dbid = (key>>16) % dbs.size();
	uint32_t cfid = (key&0xffff) % cfs[dbid].size();
	std::string val = genRandom(512);
	std::stringstream ss;
	ss << key;
	std::string keystr = ss.str();
	auto rockskey = rocksdb::Slice(keystr.c_str(), keystr.size());
	auto rocksval = rocksdb::Slice(val.c_str(), val.size());
	auto s = dbs[dbid]->Put(rocksdb::WriteOptions(), cfs[dbid][cfid].get(), rockskey, rocksval);
	assert(s.ok());
}

void odoPut(const ODBS& dbs, const CFS& cfs) {
	uint32_t key = intRand();
	uint32_t dbid = (key>>16) % dbs.size();
	uint32_t cfid = (key&0xffff) % cfs[dbid].size();
	std::string val = genRandom(512);
	std::stringstream ss;
	ss << key;
	std::string keystr = ss.str();
	auto rockskey = rocksdb::Slice(keystr.c_str(), keystr.size());
	auto rocksval = rocksdb::Slice(val.c_str(), val.size());

    rocksdb::OptimisticTransactionOptions txnOpts;

    txnOpts.set_snapshot = true;
    auto txn = dbs[dbid]->BeginTransaction(rocksdb::WriteOptions(), txnOpts);
    txn->Put(cfs[dbid][cfid].get(), rockskey, rocksval);
    txn->Commit();
    delete txn;
}

void pdoPut(const PDBS& dbs, const CFS& cfs) {
	uint32_t key = intRand();
	uint32_t dbid = (key>>16) % dbs.size();
	uint32_t cfid = (key&0xffff) % cfs[dbid].size();
	std::string val = genRandom(512);
	std::stringstream ss;
	ss << key;
	std::string keystr = ss.str();
	auto rockskey = rocksdb::Slice(keystr.c_str(), keystr.size());
	auto rocksval = rocksdb::Slice(val.c_str(), val.size());

    rocksdb::TransactionOptions txn_options;
    auto txn = dbs[dbid]->BeginTransaction(rocksdb::WriteOptions(), txn_options);
    txn->Put(cfs[dbid][cfid].get(), rockskey, rocksval);
    txn->Commit();
    delete txn;
}

void doGet(const DBS& dbs, const CFS& cfs) {
	uint32_t key = intRand();
	uint32_t dbid = (key>>16) % dbs.size();
	uint32_t cfid = (key&0xffff) % cfs[dbid].size();
	std::stringstream ss;
	ss << key;
	std::string keystr = ss.str();
	auto rockskey = rocksdb::Slice(keystr.c_str(), keystr.size());
	std::string val;
	dbs[dbid]->Get(rocksdb::ReadOptions(), cfs[dbid][cfid].get(), ss.str(), &val);
}

void odoGet(const ODBS& dbs, const CFS& cfs) {
	uint32_t key = intRand();
	uint32_t dbid = (key>>16) % dbs.size();
	uint32_t cfid = (key&0xffff) % cfs[dbid].size();
	std::stringstream ss;
	ss << key;
	std::string keystr = ss.str();
	auto rockskey = rocksdb::Slice(keystr.c_str(), keystr.size());
	std::string val;

    rocksdb::OptimisticTransactionOptions txnOpts;
    txnOpts.set_snapshot = true;
    auto txn = dbs[dbid]->BeginTransaction(rocksdb::WriteOptions(), txnOpts);
    txn->Get(rocksdb::ReadOptions(), cfs[dbid][cfid].get(), ss.str(), &val);
    delete txn;
}

void pdoGet(const PDBS& dbs, const CFS& cfs) {
	uint32_t key = intRand();
	uint32_t dbid = (key>>16) % dbs.size();
	uint32_t cfid = (key&0xffff) % cfs[dbid].size();
	std::stringstream ss;
	ss << key;
	std::string keystr = ss.str();
	auto rockskey = rocksdb::Slice(keystr.c_str(), keystr.size());
	std::string val;

    rocksdb::TransactionOptions txnOpts;
    auto txn = dbs[dbid]->BeginTransaction(rocksdb::WriteOptions(), txnOpts);
    txn->Get(rocksdb::ReadOptions(), cfs[dbid][cfid].get(), ss.str(), &val);
    delete txn;
}

void loadFunctor(const DBS& dbs, const CFS& cfs, int thd_id, int record_num) {
	for (int i = 0; i < record_num; ++i) {
		doPut(dbs, cfs);
		if (i % 10000 == 0) {
			std::cout << "thd:" << thd_id << ",insert num:" << i << std::endl;
		}
	}
	std::cout << "thd:" << thd_id << ", load done" << std::endl;
}

void oloadFunctor(const ODBS& dbs, const CFS& cfs, int thd_id, int record_num) {
	for (int i = 0; i < record_num; ++i) {
		odoPut(dbs, cfs);
		if (i % 10000 == 0) {
			std::cout << "thd:" << thd_id << ",insert num:" << i << std::endl;
		}
	}
	std::cout << "thd:" << thd_id << ", load done" << std::endl;
}

void ploadFunctor(const PDBS& dbs, const CFS& cfs, int thd_id, int record_num) {
    for (int i = 0; i < record_num; ++i) {
        pdoPut(dbs, cfs);
		if (i % 10000 == 0) {
			std::cout << "thd:" << thd_id << ",insert num:" << i << std::endl;
		}
    }
	std::cout << "thd:" << thd_id << ", load done" << std::endl;
}

void runFunctor(const DBS& dbs, const CFS& cfs, int thd_id, int record_num) {
	for (int i = 0; i < record_num; ++i) {
		if (intRand() % 100 >= 50) {
			doGet(dbs, cfs);
		} else {
			doPut(dbs, cfs);
		}
		if (i % 10000 == 0) {
			std::cout << "thd:" << thd_id << ",op num:" << i << std::endl;
		}
	}
	std::cout << "thd:" << thd_id << ", run done" << std::endl;
}

void orunFunctor(const ODBS& dbs, const CFS& cfs, int thd_id, int record_num) {
	for (int i = 0; i < record_num; ++i) {
		if (intRand() % 100 >= 50) {
			odoGet(dbs, cfs);
		} else {
			odoPut(dbs, cfs);
		}
		if (i % 10000 == 0) {
			std::cout << "thd:" << thd_id << ",op num:" << i << std::endl;
		}
	}
	std::cout << "thd:" << thd_id << ", run done" << std::endl;
}

void prunFunctor(const PDBS& dbs, const CFS& cfs, int thd_id, int record_num) {
	for (int i = 0; i < record_num; ++i) {
		if (intRand() % 100 >= 50) {
			pdoGet(dbs, cfs);
		} else {
			pdoPut(dbs, cfs);
		}
		if (i % 10000 == 0) {
			std::cout << "thd:" << thd_id << ",op num:" << i << std::endl;
		}
	}
	std::cout << "thd:" << thd_id << ", run done" << std::endl;
}

void monitorFunctor(const std::vector<rocksdb::DB*> dbs, const CFS& cfs, const std::atomic<bool>& stop_flag) {
        std::vector<std::pair<std::string, uint64_t>> properties = { 
            {"num-immutable-mem-table", 0},
            {"mem-table-flush-pending", 0},
            {"compaction-pending", 0},
            {"cur-size-active-mem-table", 0},
            {"cur-size-all-mem-tables", 0},
            {"estimate-table-readers-mem", 0}};
	for ( ;!stop_flag.load(std::memory_order_relaxed);) {
		std::ofstream myfile;
  		myfile.open("/tmp/rocks_main", std::ofstream::out|std::ofstream::trunc);
		for (auto& property : properties) {
		    uint64_t tmp = 0;
		    property.second = 0;
		    for (size_t i = 0; i < dbs.size(); i++) {
			for (size_t j = 0; j < cfs[i].size(); j++) {
				dbs[i]->GetIntProperty(cfs[i][j].get(), "rocksdb." + property.first, &tmp);
		    		property.second += tmp;
			}
		    }
	  	    myfile << property.second << ' ';
		}
		myfile.close();
		sleep(1);
	}
}

int main(int argc, char *argv[]) {
	if (argc != 9) {
		usage();
		return 0;
	}

    std::string mode;
	int db_num = -1;
	int cf_num = -1;
	int thd_num = -1;
	int memtable_num = -1;
	int memtable_size = -1;
	int record_num = -1;
	bool is_load = 0;
    mode = argv[1];
    if (mode != "db" && mode != "odb" && mode != "pdb") {
        std::cout << "invalid db param" << std::endl;
        return 0;
    }
	if (std::string(argv[2]) == "load") {
		is_load = 1;
	}
	std::stringstream(argv[3]) >> db_num;
	std::stringstream(argv[4]) >> cf_num;
	std::stringstream(argv[5]) >> thd_num;
	std::stringstream(argv[6]) >> memtable_num;
	std::stringstream(argv[7]) >> memtable_size;
	std::stringstream(argv[8]) >> record_num;
	if (db_num == -1 || cf_num == -1 || thd_num == -1 || record_num == -1 || memtable_num == -1 || memtable_size == -1) {
		std::cout << "invalid parameter" << std::endl;
		return 0;
	}
	assert(memtable_num >= 0 && memtable_num <= 4);
	assert(memtable_size >= 8*1024*1024);
    auto bc = rocksdb::NewLRUCache(4 * 1024 * 1024 * 1024LL, 6);

	DBS dbs;
    ODBS odbs;
    PDBS pdbs;
	CFS cfs;
	for (int i = 0; i < db_num; i++) {
		cfs.emplace_back(std::vector<std::unique_ptr<rocksdb::ColumnFamilyHandle>>());
	}
	for (int i = 0; i < db_num; i++) {
		std::stringstream s;
		s << "./db/" << i;
		rocksdb::DB* db;
        rocksdb::OptimisticTransactionDB* odb;
        rocksdb::TransactionDB* pdb;
		rocksdb::Status status;
		if (is_load) {
            if (mode == "db") {
			    status = rocksdb::DB::Open(_options(bc, memtable_num, memtable_size), s.str(), &db);
            } else if (mode == "pdb") {
                status = rocksdb::TransactionDB::Open(_options(bc, memtable_num, memtable_size),
                       rocksdb::TransactionDBOptions(), s.str(), &pdb);
            } else if (mode == "odb") {
                status = rocksdb::OptimisticTransactionDB::Open(_options(bc, memtable_num, memtable_size), s.str(), &odb);
            }
            assert(status.ok());
			for (int j = 0; j < cf_num; j++) {
				rocksdb::ColumnFamilyHandle* cfHandler;
				std::stringstream cf_s;
				cf_s << "db_" << i << "_cf_" << j;
                if (mode == "db") {
				    status = db->CreateColumnFamily(_cfOptions(memtable_num, memtable_size), cf_s.str(), &cfHandler);
                } else if (mode == "pdb") {
				    status = pdb->CreateColumnFamily(_cfOptions(memtable_num, memtable_size), cf_s.str(), &cfHandler);
                } else if (mode == "odb") {
				    status = odb->GetBaseDB()->CreateColumnFamily(_cfOptions(memtable_num, memtable_size), cf_s.str(), &cfHandler);
                }
				cfs[i].emplace_back(std::unique_ptr<rocksdb::ColumnFamilyHandle>(cfHandler));
				assert(status.ok());
			}
            if (mode == "db") {
			    dbs.emplace_back(std::unique_ptr<rocksdb::DB>(db));
            } else if (mode == "odb") {
                odbs.emplace_back(std::unique_ptr<rocksdb::OptimisticTransactionDB>(odb));
            } else if (mode == "pdb") {
                pdbs.emplace_back(std::unique_ptr<rocksdb::TransactionDB>(pdb));
            }
		} else {
			std::vector<std::string> cfList;
			status = rocksdb::DB::ListColumnFamilies(_options(bc, memtable_num, memtable_size), s.str(), &cfList);
			assert(status.ok());
			std::vector<rocksdb::ColumnFamilyDescriptor> cfDescList;
			std::vector<rocksdb::ColumnFamilyHandle*> handles;
			for (auto& v : cfList) {
				cfDescList.emplace_back(rocksdb::ColumnFamilyDescriptor(v, _cfOptions(memtable_num, memtable_size)));
			}
            if (mode == "db") {
			    status = rocksdb::DB::Open(_options(bc, memtable_num, memtable_size), s.str(), cfDescList, &handles, &db);
            } else if (mode == "pdb") {
			    status = rocksdb::TransactionDB::Open(
                        _options(bc, memtable_num, memtable_size),
                        rocksdb::TransactionDBOptions(),
                        s.str(), cfDescList, &handles, &pdb);
            } else if (mode == "odb") {
			    status = rocksdb::OptimisticTransactionDB::Open(_options(bc, memtable_num,
                        memtable_size), s.str(), cfDescList, &handles, &odb);
            }
			assert(status.ok());
			for(auto &v : handles) {
				cfs[i].emplace_back(std::unique_ptr<rocksdb::ColumnFamilyHandle>(v));
			}
            if (mode == "db") {
			    dbs.emplace_back(std::unique_ptr<rocksdb::DB>(db));
            } else if (mode == "odb") {
                odbs.emplace_back(std::unique_ptr<rocksdb::OptimisticTransactionDB>(odb));
            } else if (mode == "pdb") {
                pdbs.emplace_back(std::unique_ptr<rocksdb::TransactionDB>(pdb));
            }
		}
	}

	std::vector<std::thread> threads;
	for (int i = 0; i < thd_num; ++i) {
        if (mode == "db") {
            if (is_load) {
                threads.emplace_back(std::thread(loadFunctor, std::ref(dbs), std::ref(cfs), i, record_num/thd_num));
            } else {
                threads.emplace_back(std::thread(runFunctor, std::ref(dbs), std::ref(cfs), i, record_num/thd_num));
            }
        } else if (mode == "odb") {
            if (is_load) {
                threads.emplace_back(std::thread(oloadFunctor, std::ref(odbs), std::ref(cfs), i, record_num/thd_num));
            } else {
                threads.emplace_back(std::thread(orunFunctor, std::ref(odbs), std::ref(cfs), i, record_num/thd_num));
            }
        } else {
            if (is_load) {
                threads.emplace_back(std::thread(ploadFunctor, std::ref(pdbs), std::ref(cfs), i, record_num/thd_num));
            } else {
                threads.emplace_back(std::thread(prunFunctor, std::ref(pdbs), std::ref(cfs), i, record_num/thd_num));
            }
        }
	}

	std::atomic<bool> stopped(false);
    std::vector<rocksdb::DB*> alldbs;
    if (mode == "db") {
        for (auto& db : dbs) {
            alldbs.push_back(db.get());
        }
    } else if (mode == "odb") {
        for (auto& db : odbs) {
            alldbs.push_back(db->GetBaseDB());
        }
    } else if (mode == "pdb") {
        for (auto& db : pdbs) {
            alldbs.push_back(db->GetBaseDB());
        }
    }
	auto monitor = std::thread(monitorFunctor, alldbs, std::ref(cfs), std::ref(stopped));

	for (auto& v : threads) {
		v.join();
	}
	stopped.store(true, std::memory_order_relaxed);
	monitor.join();
	return 0;
}
