#include <assert.h>
#include <sstream>
#include <iostream>
#include <string>

#include "asio.hpp"
#include "glog/logging.h"
#include "tendisplus/network/blocking_tcp_client.h"

namespace tendisplus {

BlockingTcpClient::BlockingTcpClient(size_t maxBufSize)
        :_inited(false),
         _socket(_ctx),
         _deadline(_ctx),
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
        asio::ip::tcp::resolver(_ctx).resolve(host, ss.str());

    _deadline.expires_from_now(timeout);

    // Set up the variable that receives the result of the asynchronous
    // operation. The error code is set to would_block to signal that the
    // operation is incomplete. Asio guarantees that its asynchronous
    // operations will never fail with would_block, so any other value in
    // ec indicates completion.
    asio::error_code ec = asio::error::would_block;

    asio::async_connect(_socket, endpoints,
        [&ec](const std::error_code& oec, asio::ip::tcp::endpoint) {
            ec = oec;
        });

    // Block until the asynchronous operation has completed.
    do {
        _ctx.run_one();
    } while (ec == asio::error::would_block);

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
        [&ec](const asio::error_code& oec, size_t) {
            ec = oec;
        });

    // Block until the asynchronous operation has completed.
    do {
        _ctx.run_one();
    } while (ec == asio::error::would_block);

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
            [&ec](const asio::error_code& oec, size_t) {
                ec = oec;
            });

        // Block until the asynchronous operation has completed.
        do {
            _ctx.run_one();
        } while (ec == asio::error::would_block);

        if (ec) {
            return {ErrorCodes::ERR_NETWORK, ec.message()};
        }
    }

    size_t inputBufSize = _inputBuf.size();
    assert(inputBufSize >= bufSize);

    std::string result;
    result.resize(bufSize);
    std::istream is(&_inputBuf);
    is.read(&result[0], bufSize);

    assert(inputBufSize == _inputBuf.size() + bufSize);

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
        [&ec](const asio::error_code& oec, size_t) {
            ec = oec;
        });

    do {
        _ctx.run_one();
    } while (ec == asio::error::would_block);

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

/*
//----------------------------------------------------------------------

int main(int argc, char* argv[])
{
  try
  {
    if (argc != 4)
    {
      std::cerr << "Usage: blocking_tcp <host> <port> <message>\n";
      return 1;
    }

    client c;
    c.connect(argv[1], argv[2], boost::posix_time::seconds(10));

    boost::posix_time::ptime time_sent =
      boost::posix_time::microsec_clock::universal_time();

    c.write_line(argv[3], boost::posix_time::seconds(10));

    for (;;)
    {
      std::string line = c.read_line(boost::posix_time::seconds(10));

      // Keep going until we get back the line that was sent.
      if (line == argv[3])
        break;
    }

    boost::posix_time::ptime time_received =
      boost::posix_time::microsec_clock::universal_time();

    std::cout << "Round trip time: ";
    std::cout << (time_received - time_sent).total_microseconds();
    std::cout << " microseconds\n";
  }
  catch (std::exception& e)
  {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}
*/
