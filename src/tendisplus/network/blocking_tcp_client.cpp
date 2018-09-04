#include <sstream>
#include <iostream>
#include <utility>
#include <memory>
#include <string>

#include "asio.hpp"
#include "glog/logging.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {

BlockingTcpClient::BlockingTcpClient(std::shared_ptr<asio::io_context> ctx,
                asio::ip::tcp::socket socket,
                size_t maxBufSize)
        :_inited(true),
         _ctx(ctx),
         _socket(std::move(socket)),
         _inputBuf(maxBufSize) {
    if (&(_socket.get_io_context()) != &(*ctx)) {
        LOG(FATAL) << " cannot transfer socket between ioctx";
    }
    std::error_code ec;
    _socket.non_blocking(true, ec);
    INVARIANT(ec.value() == 0);
    _socket.set_option(asio::ip::tcp::no_delay(true));
    _socket.set_option(asio::socket_base::keep_alive(true));
}

BlockingTcpClient::BlockingTcpClient(std::shared_ptr<asio::io_context> ctx,
                size_t maxBufSize)
        :_inited(false),
         _ctx(ctx),
         _socket(*_ctx),
         _inputBuf(maxBufSize) {
    std::error_code ec;
    _socket.non_blocking(true, ec);
    INVARIANT(ec.value() == 0);
    _socket.set_option(asio::ip::tcp::no_delay(true));
    _socket.set_option(asio::socket_base::keep_alive(true));
}

void BlockingTcpClient::closeSocket() {
    asio::error_code ignoredEc;
    _socket.close(ignoredEc);
}

Status BlockingTcpClient::connect(const std::string& host, uint16_t port,
        std::chrono::seconds timeout) {
    {
        std::lock_guard<std::mutex> lk(_mutex);
        if (_inited) {
            return {ErrorCodes::ERR_NETWORK, "already inited sock"};
        }
        _inited = true;
    }
    std::stringstream ss;
    ss << port;
    asio::ip::tcp::resolver::results_type endpoints =
        asio::ip::tcp::resolver(*_ctx).resolve(host, ss.str());

    asio::error_code ec;
    bool notified = false;
    asio::async_connect(_socket, endpoints,
        [this, &ec, &notified](const std::error_code& oec,
                                asio::ip::tcp::endpoint) {
            std::unique_lock<std::mutex> lk(_mutex);
            ec = oec;
            notified = true;
            _cv.notify_one();
        });

    std::unique_lock<std::mutex> lk(_mutex);
    if (_cv.wait_for(lk, timeout, [&notified]{ return notified;})) {
        if (ec) {
            closeSocket();
            return {ErrorCodes::ERR_NETWORK, ec.message()};
        }
        return {ErrorCodes::ERR_OK, ""};
    } else {
        closeSocket();
        return {ErrorCodes::ERR_TIMEOUT, ""};
    }
}

Expected<std::string> BlockingTcpClient::readLine(
        std::chrono::seconds timeout) {
    asio::error_code ec;
    bool notified = false;
    asio::async_read_until(_socket, _inputBuf, "\n",
        [this, &ec, &notified](const asio::error_code& oec, size_t size) {
            std::unique_lock<std::mutex> lk(_mutex);
            ec = oec;
            notified = true;
            _cv.notify_one();
        });

    std::unique_lock<std::mutex> lk(_mutex);
    if (_cv.wait_for(lk, timeout, [&notified]{ return notified;})) {
        if (ec) {
            closeSocket();
            return {ErrorCodes::ERR_NETWORK, ec.message()};
        }

        std::string line;
        std::istream is(&_inputBuf);
        std::getline(is, line);
        if (line[line.size()-1] != '\r') {
            closeSocket();
            return {ErrorCodes::ERR_NETWORK, "line not ended with \\r\\n"};
        }
        line.erase(line.size()-1);
        return line;
    } else {
        closeSocket();
        return {ErrorCodes::ERR_TIMEOUT, ""};
    }
}

// TODO(deyukong): unittest read after read_until works as expected
// TODO(deyukong): reduce copy times
Expected<std::string> BlockingTcpClient::read(size_t bufSize,
        std::chrono::seconds timeout) {
    if (bufSize > _inputBuf.max_size()) {
        return {ErrorCodes::ERR_NETWORK, "read size can't exceed bufsize"};
    }

    size_t remain = bufSize > _inputBuf.size() ?
            bufSize - _inputBuf.size() : 0;

    if (remain > 0) {
        asio::error_code ec = asio::error::would_block;
        bool notified = false;

        asio::async_read(_socket, _inputBuf, asio::transfer_exactly(remain),
            [this, &ec, &notified](const asio::error_code& oec, size_t) {
                std::unique_lock<std::mutex> lk(_mutex);
                ec = oec;
                notified = true;
                _cv.notify_one();
            });

        // Block until the asynchronous operation has completed.
        std::unique_lock<std::mutex> lk(_mutex);
        if (!_cv.wait_for(lk, timeout, [&notified]{ return notified;})) {
            closeSocket();
            return {ErrorCodes::ERR_TIMEOUT, ""};
        } else if (ec) {
            closeSocket();
            return {ErrorCodes::ERR_NETWORK, ec.message()};
        } else {
            // everything is ok, stepout this scope and process buffer
        }
    }

    size_t inputBufSize = _inputBuf.size();
    INVARIANT(inputBufSize >= bufSize);

    std::string result;
    result.resize(bufSize);
    std::istream is(&_inputBuf);
    is.read(&result[0], bufSize);

    INVARIANT(inputBufSize == _inputBuf.size() + bufSize);

    return result;
}

Status BlockingTcpClient::writeData(const std::string& data,
        std::chrono::seconds timeout) {
    asio::error_code ec = asio::error::would_block;
    bool notified = false;
    asio::async_write(_socket, asio::buffer(data),
        [this, &ec, &notified](const asio::error_code& oec, size_t) {
            std::unique_lock<std::mutex> lk(_mutex);
            ec = oec;
            notified = true;
            _cv.notify_one();
        });

    std::unique_lock<std::mutex> lk(_mutex);
    if (_cv.wait_for(lk, timeout, [&notified]{ return notified;})) {
        if (ec) {
            closeSocket();
            return {ErrorCodes::ERR_NETWORK, ec.message()};
        } else {
            return {ErrorCodes::ERR_OK, ""};
        }
    } else {
        closeSocket();
        return {ErrorCodes::ERR_TIMEOUT, ""};
    }
}

Status BlockingTcpClient::writeLine(const std::string& line,
        std::chrono::seconds timeout) {
    std::string line1 = line;
    line1.append("\r\n");
    return writeData(line1, timeout);
}

asio::ip::tcp::socket BlockingTcpClient::borrowConn() {
    return std::move(_socket);
}

}  // namespace tendisplus
