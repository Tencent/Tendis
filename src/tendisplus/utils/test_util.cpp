// Copyright [2019] <eliotwang@tencent.com>
#include <fstream>
#include <utility>

#include "gtest/gtest.h"
#include "glog/logging.h"
#include "tendisplus/utils/test_util.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/portable.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/storage/rocks/rocks_kvstore.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/string.h"

namespace tendisplus {
std::shared_ptr<ServerParams> makeServerParam(uint32_t port, uint32_t storeCnt, const std::string& dir) {
    const auto guard = MakeGuard([]{
        remove("test.cfg");
    });

    std::ofstream myfile;
    myfile.open("test.cfg");
    myfile << "bind 127.0.0.1\n";
    myfile << "port " << port << "\n";
    myfile << "loglevel debug\n";
    if (dir != "") {
        myfile << "logdir ./" << dir <<"/log\n";
        myfile << "dir ./" << dir <<"/db\n";
        myfile << "dumpdir ./" << dir <<"/dump\n";
        myfile << "pidfile ./" << dir << "/tendisplus.pid\n";
    } else {
        myfile << "logdir ./log\n";
        myfile << "dir ./db\n";
        myfile << "dumpdir ./dump\n";
        myfile << "pidfile ./tendisplus.pid\n";
    }
    myfile << "storage rocks\n";
    myfile << "rocks.blockcachemb 4096\n";
    myfile << "generallog on\n";
    if (storeCnt != 0) {
        myfile << "kvStoreCount "<< storeCnt << "\n";
    }
    myfile.close();

    auto cfg = std::make_shared<ServerParams>();
    auto s = cfg->parseFile("test.cfg");
    EXPECT_EQ(s.ok(), true);
    return cfg;
}

bool setupEnv() {
    std::error_code ec;

    filesystem::remove_all("./log", ec);
    // EXPECT_EQ(ec.value(), 0);
    EXPECT_TRUE(ec.value() == 0 || ec.value() == 2);

    filesystem::remove_all("./db", ec);
    // EXPECT_EQ(ec.value(), 0);
    EXPECT_TRUE(ec.value() == 0 || ec.value() == 2);

    EXPECT_TRUE(filesystem::create_directory("./db"));
    EXPECT_TRUE(filesystem::create_directory("./log"));

    return true;
}

void destroyEnv() {
    std::error_code ec;
    filesystem::remove_all("./log", ec);
    filesystem::remove_all("./db", ec);
}

bool setupReplEnv() {
    std::error_code ec;
    auto vec = { "master", "slave" };

    for (auto v : vec) {
        std::stringstream ss;
        ss << "./" << v << "/log";
        filesystem::remove_all(ss.str(), ec);
        EXPECT_TRUE(ec.value() == 0 || ec.value() == 2);
        EXPECT_TRUE(filesystem::create_directories(ss.str()));

        ss.str("");
        ss << "./" << v << "/db";
        filesystem::remove_all(ss.str(), ec);
        EXPECT_TRUE(ec.value() == 0 || ec.value() == 2);
        EXPECT_TRUE(filesystem::create_directories(ss.str()));
    }

    return true;
}

bool destroyReplEnv() {
    std::error_code ec;
    auto vec = { "master", "slave" };

    for (auto v : vec) {
        std::stringstream ss;
        ss << "./" << v << "/log";
        filesystem::remove_all(ss.str(), ec);
        EXPECT_TRUE(ec.value() == 0 || ec.value() == 2);

        ss.str("");
        ss << "./" << v << "/db";
        filesystem::remove_all(ss.str(), ec);
        EXPECT_TRUE(ec.value() == 0 || ec.value() == 2);
    }

    return true;
}


std::string getBulkValue(const std::string& reply, uint32_t index) {
    INVARIANT(index == 0);
    auto ptr = reply.c_str();
    std::string buf;
    buf.reserve(128);

    size_t i = 0;
    size_t size = 0;

    if (ptr[i] == '*') {
        while (ptr[++i] != '\r');

        i += 2; // skip the '\n'
    }

    switch (ptr[i]) {
    case '$':
        while (ptr[++i] != '\r') {
            buf.append(1, ptr[i]);
        }
        size = std::stol(buf);
        i += 2; // skip the '\n'
        break;
    default:
        INVARIANT(0);
        break;
    }
    buf.clear();
    INVARIANT(reply.size() > i + size);
    buf.insert(buf.end(), reply.begin() + i, reply.begin() + i + size);
    INVARIANT(ptr[i + size] == '\r');

    return buf;
}

std::shared_ptr<ServerEntry> makeServerEntry(
    const std::shared_ptr<ServerParams>& cfg) {
    auto block_cache =
        rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 4);
    auto server = std::make_shared<ServerEntry>(cfg);

    uint32_t kvStoreCount = cfg->kvStoreCount;
    uint32_t chunkSize = cfg->chunkSize;

    // catalog init
    auto catalog = std::make_unique<Catalog>(
        std::move(std::unique_ptr<KVStore>(
            new RocksKVStore(CATALOG_NAME, cfg, nullptr))),
        kvStoreCount, chunkSize);
    server->installCatalog(std::move(catalog));

    std::vector<PStore> tmpStores;
    for (size_t dbId = 0; dbId < kvStoreCount; ++dbId) {
        KVStore::StoreMode mode = KVStore::StoreMode::READ_WRITE;

        auto meta = server->getCatalog()->getStoreMainMeta(dbId);
        if (meta.ok()) {
            mode = meta.value()->storeMode;
        }
        else if (meta.status().code() == ErrorCodes::ERR_NOTFOUND) {
            auto pMeta = std::unique_ptr<StoreMainMeta>(
                new StoreMainMeta(dbId, KVStore::StoreMode::READ_WRITE));
            Status s = server->getCatalog()->setStoreMainMeta(*pMeta);
            if (!s.ok()) {
                LOG(FATAL) << "catalog setStoreMainMeta error:"
                    << s.toString();
                return nullptr;
            }
        }
        else {
            LOG(FATAL) << "catalog getStoreMainMeta error:"
                << meta.status().toString();
            return nullptr;
        }

        tmpStores.emplace_back(std::unique_ptr<KVStore>(
                new RocksKVStore(std::to_string(dbId), cfg,
                    block_cache, mode)));
    }
    server->installStoresInLock(tmpStores);
    auto seg_mgr = std::unique_ptr<SegmentMgr>(
        new SegmentMgrFnvHash64(tmpStores, chunkSize));
    server->installSegMgrInLock(std::move(seg_mgr));

    auto tmpPessimisticMgr = std::make_unique<PessimisticMgr>(
        kvStoreCount);
    server->installPessimisticMgrInLock(std::move(tmpPessimisticMgr));

    auto tmpMGLockMgr = std::make_unique <mgl::MGLockMgr>();
    server->installMGLockMgrInLock(std::move(tmpMGLockMgr));

    return server;
}

std::shared_ptr<NetSession> makeSession(
    std::shared_ptr<ServerEntry> server,
    std::shared_ptr<asio::io_context> ctx) {
    asio::ip::tcp::socket socket(*ctx);
    return std::make_shared<NetSession>(
        server, std::move(socket), 0, false, nullptr, nullptr);
}

std::string randomKey(size_t maxlen) {
    std::string key;

    int len = std::rand() % maxlen + 1;
    for (int i = 0; i < len; ++i) {
        key.push_back(std::rand()%('z'-'a')+(std::rand()%2?'A':'a'));
    }

    return key;
}

KeysWritten WorkLoad::writeWork(RecordType type,
                                uint32_t count,
                                uint32_t maxlen,
                                bool sharename) {
    uint32_t total = 0;
    KeysWritten keys;

    for (uint32_t i = 0; i < count; ++i) {
        std::string key = randomKey(32) + "_" + std::to_string(i);
        //if (!sharename) {
            key.push_back('_');
            key.push_back(static_cast<char>(rt2Char(type)));
        //}

        if (type == RecordType::RT_KV) {
            _session->setArgs({"set", key, std::to_string(i)});
            auto expect = Command::runSessionCmd(_session.get());
            EXPECT_TRUE(expect.status().ok());
            if (expect.status().ok()) {
                EXPECT_EQ(expect.value(), Command::fmtOK());
            }
            total++;
        } else {
            uint32_t len = static_cast<uint32_t>(std::rand() % maxlen) + 1;

            for (uint32_t j = 0; j < len; ++j) {
                if (type == RecordType::RT_LIST_META) {
                    _session->setArgs({"lpush", key, std::to_string(j)});
                    auto expect = Command::runSessionCmd(_session.get());
                    EXPECT_TRUE(expect.status().ok());
                    if (expect.status().ok()) {
                        EXPECT_EQ(expect.value(),
                              Command::fmtLongLong(static_cast<uint64_t>(j+1)));
                    }
                } else  {
                    if (type == RecordType::RT_HASH_META) {
                        _session->setArgs({"hset", key,
                                          "subkey" + std::to_string(j),
                                          std::to_string(j)});
                    } else if (type == RecordType::RT_SET_META) {
                        _session->setArgs({"sadd", key, std::to_string(j)});
                    } else if (type == RecordType::RT_ZSET_META) {
                        _session->setArgs({"zadd", key,
                                          std::to_string(std::rand()),
                                          std::to_string(j)});
                    } else {
                        INVARIANT(0);
                    }

                    auto expect = Command::runSessionCmd(_session.get());
                    EXPECT_TRUE(expect.status().ok());
                    if (expect.status().ok()) {
                        EXPECT_EQ(expect.value(), Command::fmtOne());
                    }
                }
            }

            if (len > 0) {
                total++;
            }
        }

        keys.emplace(key);
    }

    LOG(WARNING) << total << " key written into";

    return keys;
}

void WorkLoad::expireKeys(const AllKeys &all_keys, uint64_t ttl) {
    for (size_t i = 0; i < all_keys.size(); ++i) {
        for (auto &key : all_keys[i]) {
            if (i > 0) {
                bool duplicate = false;
                for (size_t j = i; j > 0; --j) {
                    auto pos = all_keys[j-1].find(key);
                    if (pos != all_keys[j-1].end()) {
                        duplicate = true;
                        break;
                    }
                }

                if (duplicate) {
                    continue;
                }
            }

            _session->setArgs({"expire", key, std::to_string(ttl)});
            auto expect = Command::runSessionCmd(_session.get());
            EXPECT_TRUE(expect.ok());
            EXPECT_EQ(expect.value(), Command::fmtOne());
        }
    }

    return;
}

void WorkLoad::slaveof(char* ip, uint32_t port) {
    _session->setArgs({ "slaveof", ip, std::to_string(port) });
    auto expect = Command::runSessionCmd(_session.get());
    EXPECT_TRUE(expect.ok());
}

Expected<uint64_t> WorkLoad::getIntResult(const std::vector<std::string>& args) {
    _session->setArgs(args);
    auto expect = Command::runSessionCmd(_session.get());
    if (!expect.ok()) {
        return expect.status();
    }

    return Command::getInt64FromFmtLongLong(expect.value());
}

std::string WorkLoad::getStringResult(const std::vector<std::string>& args) {
    _session->setArgs(args);
    auto expect = Command::runSessionCmd(_session.get());
    EXPECT_TRUE(expect.ok());

    return expect.value();
}

void WorkLoad::delKeys(const KeysWritten& keys) {
    for (auto &key : keys) {
        _session->setArgs({"del", key});
        auto expect = Command::runSessionCmd(_session.get());
        EXPECT_TRUE(expect.ok());
    }
}

int genRand() {
    int grand = 0;
    uint32_t ms = nsSinceEpoch();
    grand = rand_r(reinterpret_cast<unsigned int *>(&ms));
    return grand;
}

std::string randomStr(size_t s, bool maybeEmpty) {
    if (s == 0) {
        s = genRand() % 256;
    }
    if (!maybeEmpty) {
        s++;
    }
    std::vector<uint8_t> v;
    for (size_t i = 0; i < s; i++) {
        v.emplace_back(genRand() % 256);
    }
    return std::string(reinterpret_cast<const char*>(v.data()), v.size());
}

}  // namespace tendisplus
