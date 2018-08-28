#ifndef SRC_TENDISPLUS_NETWORK_BLOCKING_TCP_CLIENT_H_
#define SRC_TENDISPLUS_NETWORK_BLOCKING_TCP_CLIENT_H_

#include <string>
#include <chrono>
#include "asio.hpp"
#include "tendisplus/utils/status.h"

namespace tendisplus {
class BlockingTcpClient {
 public:
    BlockingTcpClient(std::shared_ptr<asio::io_context> ctx, size_t maxBufSize);
    BlockingTcpClient(std::shared_ptr<asio::io_context> ctx,
        asio::ip::tcp::socket, size_t maxBufSize);
    Status connect(const std::string& host, uint16_t port,
        std::chrono::seconds timeout);
    Expected<std::string> readLine(std::chrono::seconds timeout);
    Expected<std::string> read(size_t bufSize, std::chrono::seconds timeout);
    Status writeLine(const std::string& line, std::chrono::seconds timeout);
    Status writeData(const std::string& data, std::chrono::seconds timeout);
    size_t getReadBufSize() const { return _inputBuf.size(); }

 private:
    void checkDeadLine(const asio::error_code& ec);
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _inited;
    std::shared_ptr<asio::io_context> _ctx;
    asio::ip::tcp::socket _socket;
    asio::steady_timer _deadline;
    asio::streambuf _inputBuf;
    // a hundred year later
    static constexpr std::chrono::seconds MAX_TIMEOUT_SEC =
        std::chrono::seconds(3153600000U);
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_NETWORK_BLOCKING_TCP_CLIENT_H_
