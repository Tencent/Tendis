#include <iostream>
#include <memory>
#include <string>
#include <algorithm>
#include "glog/logging.h"
#include "tendisplus/network/network.h"
#include "tendisplus/utils/redis_port.h"

namespace tendisplus {

using asio::ip::tcp;

constexpr ssize_t REDIS_IOBUF_LEN = (1024*16);
constexpr ssize_t REDIS_MAX_QUERYBUF_LEN = (1024*1024*1024);
constexpr ssize_t REDIS_INLINE_MAX_SIZE = (1024*64);

std::string NetworkMatrix::toString() const {
    std::stringstream ss;
    ss << "\nstickPackets\t" << stickPackets
        << "\nconnCreated\t" << connCreated
        << "\nconnReleased\t" << connReleased
        << "\ninvalidPackets\t" << invalidPackets;
    return ss.str();
}

NetworkMatrix NetworkMatrix::operator-(const NetworkMatrix& right) {
    NetworkMatrix result;
    result.stickPackets = stickPackets - right.stickPackets;
    result.connCreated = connCreated - right.connCreated;
    result.connReleased = connReleased - right.connReleased;
    result.invalidPackets = invalidPackets - right.invalidPackets;
    return result;
}

NetworkAsio::NetworkAsio(std::shared_ptr<ServerEntry> server,
        std::shared_ptr<NetworkMatrix> matrix)
    :_connCreated(0),
     _server(server),
     _acceptCtx(std::make_unique<asio::io_context>()),
     _acceptor(nullptr),
     _acceptThd(nullptr),
     _isRunning(false),
     _matrix(matrix) {
}

Status NetworkAsio::prepare(const std::string& ip, const uint16_t port) {
    asio::ip::address address = asio::ip::make_address(ip);
    auto ep = tcp::endpoint(address, port);
    std::error_code ec;
    _acceptor = std::make_unique<tcp::acceptor>(*_acceptCtx, ep);
    _acceptor->set_option(tcp::acceptor::reuse_address(true));
    _acceptor->non_blocking(true, ec);
    if (ec.value()) {
        return {ErrorCodes::ERR_NETWORK, ec.message()};
    }
    return {ErrorCodes::ERR_OK, ""};
}

void NetworkAsio::doAccept() {
    _acceptor->async_accept([this](std::error_code ec, tcp::socket socket) {
        if (!_isRunning.load(std::memory_order_relaxed)) {
            LOG(INFO) << "acceptCb, server is shuting down";
            return;
        }
        if (ec.value()) {
            LOG(WARNING) << "acceptCb errorcode:" << ec.message();
            // we log this error, but dont return
        }
        _server->addSession(
            std::move(
                std::make_unique<NetSession>(
                    _server, std::move(socket), ++_connCreated,
                    true, _matrix)));
        ++_matrix->connCreated;
        doAccept();
    });
}

void NetworkAsio::stop() {
    LOG(INFO) << "network-asio begin stops...";
    _isRunning.store(false, std::memory_order_relaxed);
    _acceptCtx->stop();
    _acceptThd->join();
    LOG(INFO) << "network-asio begin stops complete...";
}

Status NetworkAsio::run() {
    _isRunning.store(true, std::memory_order_relaxed);
    _acceptThd = std::make_unique<std::thread>([this] {
        // TODO(deyukong): set threadname for debug/profile
        while (_isRunning.load(std::memory_order_relaxed)) {
            // if no work-gurad, the run() returns immediately if no other tasks
            asio::io_context::work work(*_acceptCtx);
            try {
                _acceptCtx->run();
            } catch (const std::exception& ex) {
                LOG(FATAL) << "accept failed:" << ex.what();
            } catch (...) {
                LOG(FATAL) << "unknown exception";
            }
        }
    });

    // TODO(deyukong): acceptor needs no explicitly listen.
    // but only through listen can we configure backlog.
    // _acceptor->listen(BACKLOG);
    doAccept();
    return {ErrorCodes::ERR_OK, ""};
}

NetSession::NetSession(std::shared_ptr<ServerEntry> server, tcp::socket sock,
    uint64_t connid, bool initSock, std::shared_ptr<NetworkMatrix> matrix)
        :_connId(connid),
         _closeAfterRsp(false),
         _server(server),
         _state(State::Created),
         _sock(std::move(sock)),
         _queryBuf(std::vector<char>()),
         _queryBufPos(0),
         _multibulklen(0),
         _bulkLen(-1),
         _args(std::vector<std::string>()),
         _respBuf(std::vector<char>()),
         _matrix(matrix) {
    if (initSock) {
        std::error_code ec;
        _sock.non_blocking(true, ec);
        assert(ec.value() == 0);
        _sock.set_option(tcp::no_delay(true));
        _sock.set_option(asio::socket_base::keep_alive(true));
        // TODO(deyukong): keep-alive params
    }
}

void NetSession::setState(State s) {
    _state.store(s, std::memory_order_relaxed);
}

std::string NetSession::getRemoteRepr() const {
    return _sock.remote_endpoint().address().to_string();
}

std::string NetSession::getLocalRepr() const {
    return _sock.local_endpoint().address().to_string();
}

void NetSession::schedule() {
    _server->schedule([this]() {
        stepState();
    });
}

void NetSession::start() {
    stepState();
}

const std::vector<std::string>& NetSession::getArgs() const {
    return _args;
}

void NetSession::setOkRsp() {
    const std::string s = "+OK\r\n";
    std::copy(s.begin(), s.end(), std::back_inserter(_respBuf));
}

void NetSession::setRspAndClose(const std::string& s) {
    _closeAfterRsp = true;

    std::stringstream ss;
    ss << "-ERR " << s << "\r\n";
    const std::string& s1 = ss.str();

    std::copy(s1.begin(), s1.end(), std::back_inserter(_respBuf));
    setState(State::DrainRsp);
    schedule();
}

// NOTE(deyukong): mainly port from redis::networking.c,
// func:processMultibulkBuffer, the unportable part (long long, int and so on)
// are all from the redis source code, quite ugly.
// FIXME(deyukong): rewrite into a more c++ like code.
void NetSession::drainReqCallback(const std::error_code& ec, size_t actualLen) {
    if (ec) {
        LOG(WARNING) << "drainReqCallback:" << ec.message();
        setState(State::End);
        schedule();
        return;
    }

    assert(_state.load(std::memory_order_relaxed) == State::DrainReq);

    _queryBufPos += actualLen;
    _queryBuf[_queryBufPos] = 0;
    if (_queryBufPos > REDIS_MAX_QUERYBUF_LEN) {
        ++_matrix->invalidPackets;
        setRspAndClose("Protocol error: too big mbulk count string");
        return;
    }
    char *newLine = nullptr;
    long long ll;  // NOLINT(runtime/int)
    int pos = 0;
    int ok = 0;
    if (_multibulklen == 0) {
        newLine = strchr(_queryBuf.data(), '\r');
        if (newLine == nullptr) {
            if (_queryBufPos > REDIS_INLINE_MAX_SIZE) {
                ++_matrix->invalidPackets;
                setRspAndClose("Protocol error: too big mbulk count string");
                return;
            }
            // not complete line
            schedule();
            return;
        }
        if (newLine - _queryBuf.data() > _queryBufPos - 2) {
            // not complete line
            schedule();
            return;
        }
        if (_queryBuf[0] != '*') {
            ++_matrix->invalidPackets;
            setRspAndClose("Protocol error: only support multilen proto");
            return;
        }
        char *newStart = _queryBuf.data() + 1;
        ok = redis_port::string2ll(newStart, newLine - newStart, &ll);
        if (!ok || ll > 1024*1024) {
            ++_matrix->invalidPackets;
            setRspAndClose("Protocol error: invalid multibulk length");
            return;
        }
        pos = newLine-_queryBuf.data()+2;
        if (ll <= 0) {
            shiftQueryBuf(pos, -1);
            schedule();
            return;
        }
        _multibulklen = ll;
    }
    assert(_multibulklen > 0);

    while (_multibulklen) {
        if (_bulkLen == -1) {
            newLine = strchr(_queryBuf.data()+pos, '\r');
            if (newLine == nullptr) {
                if (_queryBufPos > REDIS_INLINE_MAX_SIZE) {
                    ++_matrix->invalidPackets;
                    setRspAndClose("Protocol error: too big bulk count string");
                    return;
                }
                break;
            }
            if (newLine - _queryBuf.data() > _queryBufPos - 2) {
                break;
            }
            if (_queryBuf.data()[pos] != '$') {
                std::stringstream s;
                ++_matrix->invalidPackets;
                s << "Protocol error: expected '$', got '"
                  << _queryBuf.data()[pos] << "'";
                setRspAndClose(s.str());
                return;
            }
            char *newStart = _queryBuf.data()+pos+1;
            ok = redis_port::string2ll(newStart, newLine - newStart, &ll);
            if (!ok || ll < 0 || ll > 512*1024*1024) {
                ++_matrix->invalidPackets;
                setRspAndClose("Protocol error: invalid bulk length");
                return;
            }
            pos += newLine-(_queryBuf.data()+pos)+2;
            // the optimization of ll >= REDIS_MBULK_BIG_ARG
            // is not ported from redis
            _bulkLen = ll;
        }
        if (_queryBufPos - pos < _bulkLen + 2) {
            // not complete
            break;
        } else {
            _args.push_back(std::string(_queryBuf.data() + pos, _bulkLen));
            pos += _bulkLen+2;
            _bulkLen = -1;
            _multibulklen -= 1;
        }
    }
    if (pos != 0) {
        shiftQueryBuf(pos, -1);
    }
    if (_multibulklen == 0) {
        setState(State::Process);
    }
    if (_queryBufPos != 0) {
        ++_matrix->stickPackets;
    }
    schedule();
}

// NOTE(deyukong): an O(n) impl of array shifting, an alternative to sdsrange,
// which also has O(n) time-complexity
void NetSession::shiftQueryBuf(ssize_t start, ssize_t end) {
    if (_queryBufPos == 0) {
        return;
    }
    int64_t newLen = 0;
    if (start < 0) {
        start = _queryBufPos + start;
        if (start < 0) {
            start = 0;
        }
    }
    if (end < 0) {
        end = _queryBufPos + end;
        if (end < 0) {
            end = 0;
        }
    }
    newLen = (start > end) ? 0 : (end-start+1);
    if (newLen != 0) {
        if (start >= _queryBufPos) {
            newLen = 0;
        } else if (end >= _queryBufPos) {
            end = _queryBufPos - 1;
            newLen = (start > end) ? 0 : (end - start) + 1;
        }
    } else {
        start = 0;
    }
    if (start && newLen) {
        memmove(_queryBuf.data(), _queryBuf.data() + start, newLen);
    }
    _queryBuf[newLen] = 0;
    _queryBufPos = newLen;
}

void NetSession::drainReq() {
    if (_queryBufPos != 0) {
        // pipelined request
        // TODO(deyukong): stat-count pipelined requests
        drainReqCallback(std::error_code(), 0);
        return;
    }

    // we may do a sync-read to reduce async-callbacks
    size_t wantLen = REDIS_IOBUF_LEN;
    // here we use >= than >, so the last element will always be 0,
    // it's convinent for c-style string search
    if (wantLen + _queryBufPos >= _queryBuf.size()) {
        // the fill should be as fast as memset in 02 mode, refer to here
        // NOLINT(whitespace/line_length) https://stackoverflow.com/questions/8848575/fastest-way-to-reset-every-value-of-stdvectorint-to-0)
        _queryBuf.resize((wantLen + _queryBufPos)*2, 0);
    }

    // TODO(deyukong): I believe async_read_some wont callback if no
    // readable-event is set on the fd or this callback will be a deadloop
    // it needs futher tests
    _sock.
        async_read_some(asio::buffer(_queryBuf.data() + _queryBufPos, wantLen),
        [this](const std::error_code& ec, size_t actualLen) {
            drainReqCallback(ec, actualLen);
        });
}

uint64_t NetSession::getConnId() const {
    return _connId;
}

void NetSession::processReq() {
    _server->processReq(_connId);
    _state.store(State::DrainRsp, std::memory_order_relaxed);
    schedule();
}

void NetSession::drainRsp() {
    asio::async_write(_sock, asio::buffer(_respBuf.data(), _respBuf.size()),
        [this](const std::error_code& ec, size_t actualLen) {
            drainRspCallback(ec, actualLen);
    });
}

void NetSession::drainRspCallback(const std::error_code& ec, size_t actualLen) {
    if (ec) {
        LOG(WARNING) << "drainReqCallback:" << ec.message();
        setState(State::End);
        schedule();
        return;
    }
    if (actualLen != _respBuf.size()) {
        LOG(FATAL) << "conn:" << _connId << ",data:" << _respBuf.data()
            << ",actualLen:" << actualLen << ",bufsize:" << _respBuf.size()
            << ",invalid drainRsp len";
    }
    if (_closeAfterRsp) {
        setState(State::End);
        schedule();
        return;
    }
    setState(State::DrainReq);
    schedule();
}

void NetSession::endSession() {
    ++_matrix->connReleased;
    // NOTE(deyukong): endSession will call destructor
    // never write any codes after endSession
    _server->endSession(_connId);
}

void NetSession::stepState() {
    if (_state.load(std::memory_order_relaxed) == State::Created) {
        setState(State::DrainReq);
    }
    auto currState = _state.load(std::memory_order_relaxed);
    switch (currState) {
        case State::DrainReq:
            drainReq();
            return;
        case State::Process:
            processReq();
            return;
        case State::DrainRsp:
            drainRsp();
            return;
        case State::End:
            endSession();
            return;
        default:
            LOG(FATAL) << "connId:" << _connId << ",invalid state:"
                << int32_t(currState);
    }
}

}  // namespace tendisplus
