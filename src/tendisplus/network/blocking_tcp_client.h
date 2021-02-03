// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_NETWORK_BLOCKING_TCP_CLIENT_H_
#define SRC_TENDISPLUS_NETWORK_BLOCKING_TCP_CLIENT_H_

#include <string>
#include <chrono>  // NOLINT
#include <memory>

#include "asio.hpp"
#include "glog/logging.h"

#include "tendisplus/utils/status.h"
#include "tendisplus/utils/rate_limiter.h"

namespace tendisplus {

class BlockingTcpClient
  : public std::enable_shared_from_this<BlockingTcpClient> {
 public:
  BlockingTcpClient(std::shared_ptr<asio::io_context> ctx,
                    size_t maxBufSize,
                    uint32_t netBatchSize = 1024 * 1024,
                    uint32_t netBatchTimeoutSec = 10,
                    uint64_t netRateLimit = 0);
  BlockingTcpClient(std::shared_ptr<asio::io_context> ctx,
                    asio::ip::tcp::socket,
                    size_t maxBufSize,
                    uint32_t netBatchSize = 1024 * 1024,
                    uint32_t netBatchTimeoutSec = 10,
                    uint64_t netRateLimit = 0);
  Status connect(const std::string& host,
                 uint16_t port,
                 std::chrono::milliseconds timeout,
                 bool isBlockingConnect = true);
  Status tryWaitConnect();
  Expected<std::string> readLine(std::chrono::seconds timeout);
  Expected<std::string> read(size_t bufSize, std::chrono::seconds timeout);
  Status writeLine(const std::string& line);
  Status writeOneBatch(const char* data,
                       uint32_t size,
                       std::chrono::seconds timeout);
  Status writeData(const std::string& data);

  std::string getRemoteRepr() const {
    try {
      if (_socket.is_open()) {
        std::stringstream ss;
        ss << _socket.remote_endpoint().address().to_string() << ":"
           << _socket.remote_endpoint().port();
        return ss.str();
      }
      return "closed conn";
    } catch (const std::exception& e) {
      return e.what();
    }
  }

  uint16_t getRemotePort() const {
    try {
      if (_socket.is_open()) {
        return _socket.remote_endpoint().port();
      }
      return 0;
    } catch (const std::exception& e) {
      LOG(ERROR) << "BlockingTcpClient::getRemotePort() exception : "
                 << e.what();
      return -1;
    }
  }

  std::string getLocalRepr() const {
    if (_socket.is_open()) {
      std::stringstream ss;
      ss << _socket.local_endpoint().address().to_string() << ":"
         << _socket.local_endpoint().port();
      return ss.str();
    }
    return "closed conn";
  }

  size_t getReadBufSize() const {
    return _inputBuf.size();
  }

  asio::ip::tcp::socket borrowConn();
  void setRateLimit(uint64_t bytesPerSecond);
  void setFlags(int64_t flags) {
    _flags |= flags;
  }
  int64_t getFlags() const {
    return _flags;
  }

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
  std::chrono::milliseconds _timeout;  // ms
  uint64_t _ctime;
  std::unique_ptr<RateLimiter> _rateLimiter;
  int64_t _flags;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_NETWORK_BLOCKING_TCP_CLIENT_H_
