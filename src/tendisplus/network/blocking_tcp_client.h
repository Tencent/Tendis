#ifndef SRC_TENDISPLUS_NETWORK_BLOCKING_TCP_CLIENT_H_
#define SRC_TENDISPLUS_NETWORK_BLOCKING_TCP_CLIENT_H_

#include <string>
#include <chrono>
#include <memory>
#include "asio.hpp"
#include "tendisplus/utils/status.h"

namespace tendisplus {
class BlockingTcpClient: public std::enable_shared_from_this<BlockingTcpClient> {
 public:
    BlockingTcpClient(std::shared_ptr<asio::io_context> ctx, size_t maxBufSize,
        uint32_t netBatchSize = 1024*1024, uint32_t netBatchTimeoutSec = 10);
    BlockingTcpClient(std::shared_ptr<asio::io_context> ctx,
        asio::ip::tcp::socket, size_t maxBufSize,
        uint32_t netBatchSize = 1024*1024, uint32_t netBatchTimeoutSec = 10);
    Status connect(const std::string& host, uint16_t port,
        std::chrono::seconds timeout);
    Expected<std::string> readLine(std::chrono::seconds timeout);
    Expected<std::string> read(size_t bufSize, std::chrono::seconds timeout);
    Status writeLine(const std::string& line);
    Status writeOneBatch(const char* data, uint32_t size, std::chrono::seconds timeout);
    Status writeData(const std::string& data);

    std::string getRemoteRepr() const {
        try {
            if (_socket.is_open()) {
                return _socket.remote_endpoint().address().to_string();
            }
            return "closed conn";
        } catch (const std::exception& e) {
            return e.what();
        }
    }

    std::string getLocalRepr() const {
        if (_socket.is_open()) {
            return _socket.local_endpoint().address().to_string();
        }
        return "closed conn";
    }

    size_t getReadBufSize() const { return _inputBuf.size(); }

    asio::ip::tcp::socket borrowConn();

 private:
    void closeSocket();
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _inited;
    bool _notified;
    asio::error_code _ec;
    std::shared_ptr<asio::io_context> _ctx;
    asio::ip::tcp::socket _socket;
    asio::streambuf _inputBuf;
    uint32_t _netBatchSize;
    uint32_t _netBatchTimeoutSec;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_NETWORK_BLOCKING_TCP_CLIENT_H_
