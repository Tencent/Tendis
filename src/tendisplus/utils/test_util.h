// Copyright [2019] <eliotwang@tencent.com>
#ifndef SRC_TENDISPLUS_UTILS_TEST_UTIL_H_
#define SRC_TENDISPLUS_UTILS_TEST_UTIL_H_

#include <memory>
#include <vector>
#include <set>
#include <string>
#include <thread>

#include "asio.hpp"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/network/network.h"

namespace tendisplus {

using TestServer = std::shared_ptr<ServerEntry>;
using TestSession = std::shared_ptr<NetSession>;
using KeysWritten = std::set<std::string>;
using AllKeys = std::vector<KeysWritten>;

bool setupEnv();
bool destroyEnv();
std::shared_ptr<ServerParams> makeServerParam();
std::shared_ptr<ServerEntry> makeServerEntry(std::shared_ptr<ServerParams> cfg);
std::shared_ptr<NetSession> makeSession(std::shared_ptr<ServerEntry> server,
                                        std::shared_ptr<asio::io_context> ctx);

class WorkLoad {
 public:
    WorkLoad(TestServer server, TestSession session) :
        _session(session),
        _max_key_len(32) { }

    void init() { std::srand(std::time(nullptr)); }
    KeysWritten writeWork(RecordType, uint32_t count,
                          uint32_t maxlen = 0, bool sharename = true);
    void expireKeys(const AllKeys &keys, uint64_t ttl);
    void delKeys(const KeysWritten &keys);
    void setMaxKeyLen(uint32_t max_key_len);

 private:
    TestSession _session;
    uint32_t _max_key_len;
};
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_UTILS_TEST_UTIL_H_
