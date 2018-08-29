#include <sstream>
#include <iostream>
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
         _deadline(*ctx),
         _inputBuf(maxBufSize) {
    if (&(_socket.get_io_context()) != &(*ctx)) {
        LOG(FATAL) << " cannot transfer socket between ioctx";
    }
    _deadline.expires_from_now(MAX_TIMEOUT_SEC);
    checkDeadLine(asio::error_code());
}

BlockingTcpClient::BlockingTcpClient(std::shared_ptr<asio::io_context> ctx,
                size_t maxBufSize)
        :_inited(false),
         _ctx(ctx),
         _socket(*_ctx),
         _deadline(*_ctx),
         _inputBuf(maxBufSize) {
    _deadline.expires_from_now(MAX_TIMEOUT_SEC);
    checkDeadLine(asio::error_code());
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

    _deadline.expires_from_now(timeout);

    // Set up the variable that receives the result of the asynchronous
    // operation. The error code is set to would_block to signal that the
    // operation is incomplete. Asio guarantees that its asynchronous
    // operations will never fail with would_block, so any other value in
    // ec indicates completion.
    asio::error_code ec = asio::error::would_block;

    asio::async_connect(_socket, endpoints,
        [this, &ec](const std::error_code& oec, asio::ip::tcp::endpoint) {
            ec = oec;
            _cv.notify_one();
        });

    std::unique_lock<std::mutex> lk(_mutex);
    _cv.wait(lk);
    _deadline.expires_from_now(MAX_TIMEOUT_SEC);
    // Determine whether a connection was successfully established. The
    // deadline actor may have had a chance to run and close our socket, even
    // though the connect operation notionally succeeded. Therefore we must
    // check whether the socket is still open before deciding if we succeeded
    // or failed.
    if (ec || !_socket.is_open()) {
        return {ErrorCodes::ERR_NETWORK, ec.message()};
    }
    return {ErrorCodes::ERR_OK, ""};
}

Expected<std::string> BlockingTcpClient::readLine(
        std::chrono::seconds timeout) {
    _deadline.expires_from_now(timeout);

    // Set up the variable that receives the result of the asynchronous
    // operation. The error code is set to would_block to signal that the
    // operation is incomplete. Asio guarantees that its asynchronous
    // operations will never fail with would_block, so any other value in
    // ec indicates completion.
    asio::error_code ec = asio::error::would_block;

    asio::async_read_until(_socket, _inputBuf, "\n",
        [this, &ec](const asio::error_code& oec, size_t) {
            ec = oec;
            _cv.notify_one();
        });

    // Block until the asynchronous operation has completed.
    std::unique_lock<std::mutex> lk(_mutex);
    _cv.wait(lk);
    _deadline.expires_from_now(MAX_TIMEOUT_SEC);

    if (ec) {
        return {ErrorCodes::ERR_NETWORK, ec.message()};
    }

    std::string line;
    std::istream is(&_inputBuf);
    std::getline(is, line);
    if (line[line.size()-1] != '\r') {
        return {ErrorCodes::ERR_NETWORK, "line not ended with \\r\\n"};
    }
    line.erase(line.size()-1);
    return line;
}

// TODO(deyukong): unittest read after read_until works as expected
// TODO(deyukong): reduce copy times
Expected<std::string> BlockingTcpClient::read(size_t bufSize,
        std::chrono::seconds timeout) {
    if (bufSize > _inputBuf.max_size()) {
        return {ErrorCodes::ERR_NETWORK, "read size can't exceed bufsize"};
    }

    _deadline.expires_from_now(timeout);

    // Set up the variable that receives the result of the asynchronous
    // operation. The error code is set to would_block to signal that the
    // operation is incomplete. Asio guarantees that its asynchronous
    // operations will never fail with would_block, so any other value in
    // ec indicates completion.
    asio::error_code ec = asio::error::would_block;

    size_t remain = bufSize > _inputBuf.size() ?
            bufSize - _inputBuf.size() : 0;

    if (remain > 0) {
        asio::async_read(_socket, _inputBuf, asio::transfer_exactly(remain),
            [this, &ec](const asio::error_code& oec, size_t) {
                ec = oec;
                _cv.notify_one();
            });

        // Block until the asynchronous operation has completed.
        std::unique_lock<std::mutex> lk(_mutex);
        _cv.wait(lk);
        _deadline.expires_from_now(MAX_TIMEOUT_SEC);

        if (ec) {
            return {ErrorCodes::ERR_NETWORK, ec.message()};
        }
    }

    size_t inputBufSize = _inputBuf.size();
    INVARIANT(inputBufSize >= bufSize);

    std::string result;
    result.resize(bufSize);
    std::istream is(&_inputBuf);
    is.read(&result[0], bufSize);

    INVARIANT(inputBufSize == _inputBuf.size() + bufSize);

    // supress compile complain
    (void)inputBufSize;

    return result;
}

Status BlockingTcpClient::writeData(const std::string& data,
        std::chrono::seconds timeout) {
    _deadline.expires_from_now(timeout);

    // Set up the variable that receives the result of the asynchronous
    // operation. The error code is set to would_block to signal that the
    // operation is incomplete. Asio guarantees that its asynchronous
    // operations will never fail with would_block, so any other value in
    // ec indicates completion.
    asio::error_code ec = asio::error::would_block;

    asio::async_write(_socket, asio::buffer(data),
        [this, &ec](const asio::error_code& oec, size_t) {
            ec = oec;
            _cv.notify_one();
        });

    std::unique_lock<std::mutex> lk(_mutex);
    _cv.wait(lk);
    _deadline.expires_from_now(MAX_TIMEOUT_SEC);

    if (ec) {
        return {ErrorCodes::ERR_NETWORK, ec.message()};
    }
    return {ErrorCodes::ERR_OK, ""};
}

Status BlockingTcpClient::writeLine(const std::string& line,
        std::chrono::seconds timeout) {
    std::string line1 = line;
    line1.append("\r\n");
    return writeData(line1, timeout);
}

void BlockingTcpClient::checkDeadLine(const asio::error_code& ec) {
    std::lock_guard<std::mutex> lk(_mutex);
    bool needClose = false;
    if (ec && ec != asio::error::operation_aborted) {
        LOG(WARNING) << "tcpclient check_deadline err:" << ec.message();
        needClose = true;
    }
    if (!needClose && _deadline.expires_from_now() <= std::chrono::seconds(0)) {
        needClose = true;
    }
    if (needClose) {
        asio::error_code ignoredEc;
        _socket.close(ignoredEc);
        _deadline.expires_from_now(MAX_TIMEOUT_SEC);
    }
    // Put the actor back to sleep.
    _deadline.async_wait([this](const asio::error_code& ec) {
        checkDeadLine(ec);
    });
}

}  // namespace tendisplus
