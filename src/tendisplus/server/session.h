#ifndef SRC_TENDISPLUS_SERVER_SESSION_H_
#define SRC_TENDISPLUS_SERVER_SESSION_H_

#include <utility>
#include <atomic>
#include <memory>
#include <string>
#include <vector>
#include "asio.hpp"
#include "tendisplus/utils/status.h"

namespace tendisplus {

class ServerEntry;
class SessionCtx;

class Session: public std::enable_shared_from_this<Session> {
 public:
    explicit Session(std::shared_ptr<ServerEntry> svr);
    virtual ~Session();
    uint64_t id() const;

    void setResponse(const std::string& s);
    const std::vector<std::string>& getArgs() const;
    SessionCtx *getCtx() const;
    std::shared_ptr<ServerEntry> getServerEntry() const;

    virtual asio::ip::tcp::socket borrowConn() = 0;
    virtual void start() = 0;
    virtual Status cancel() = 0;
    virtual std::string getRemoteRepr() const = 0;
    virtual std::string getLocalRepr() const = 0;

 protected:
    std::vector<std::string> _args;
    std::vector<char> _respBuf;
    std::shared_ptr<ServerEntry> _server;
    std::unique_ptr<SessionCtx> _ctx;

 private:
    uint64_t _sessId;
    static std::atomic<uint64_t> _idGen;
    static std::atomic<uint64_t> _aliveCnt;
};

}  // namespace tendisplus
#endif  // SRC_TENDISPLUS_SERVER_SESSION_H_
