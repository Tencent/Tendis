#ifndef SRC_TENDISPLUS_NETWORK_NETWORK_H_
#define SRC_TENDISPLUS_NETWORK_NETWORK_H_

#include <utility>
#include <atomic>
#include <memory>
#include <string>
#include "asio.hpp"
#include "tendisplus/utils/status.h"

namespace tendisplus {
class NetworkAsio {
 public:
    NetworkAsio();
    NetworkAsio(const NetworkAsio&) = delete;
    NetworkAsio(NetworkAsio&&) = delete;
    Status prepare(const std::string& ip, const uint16_t port);
    Status run();
    void stop();
 private:
    void doAccept();
    std::unique_ptr<asio::io_context> _acceptCtx;
    std::unique_ptr<asio::io_context> _workCtx;
    std::unique_ptr<asio::ip::tcp::acceptor> _acceptor;
    std::unique_ptr<std::thread> _acceptThd;
    std::atomic<bool> _isRunning;
};

}  // namespace tendisplus
#endif  // SRC_TENDISPLUS_NETWORK_NETWORK_H_
