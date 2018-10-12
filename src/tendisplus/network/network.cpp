#include <iostream>
#include <memory>
#include <string>
#include <algorithm>
#include "glog/logging.h"
#include "tendisplus/network/network.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {

using asio::ip::tcp;

constexpr ssize_t REDIS_IOBUF_LEN = (1024*16);
constexpr ssize_t REDIS_MAX_QUERYBUF_LEN = (1024*1024*1024);
constexpr ssize_t REDIS_INLINE_MAX_SIZE = (1024*64);

std::string RequestMatrix::toString() const {
    std::stringstream ss;
    ss << "\nprocessed\t" << processed
        << "\nreadPacketCost\t" << readPacketCost << "ns"
        << "\nprocessCost\t" << processCost << "ns"
        << "\nsendPacketCost\t" << sendPacketCost << "ns";
    return ss.str();
}

RequestMatrix RequestMatrix::operator-(const RequestMatrix& right) {
    RequestMatrix result;
    result.processed = processed - right.processed;
    result.readPacketCost = readPacketCost - right.readPacketCost;
    result.processCost = processCost - right.processCost;
    result.sendPacketCost = sendPacketCost - right.sendPacketCost;
    return result;
}

std::string NetworkMatrix::toString() const {
    std::stringstream ss;
    ss << "\nstickyPackets\t" << stickyPackets
        << "\nconnCreated\t" << connCreated
        << "\nconnReleased\t" << connReleased
        << "\ninvalidPackets\t" << invalidPackets;
    return ss.str();
}

NetworkMatrix NetworkMatrix::operator-(const NetworkMatrix& right) {
    NetworkMatrix result;
    result.stickyPackets = stickyPackets - right.stickyPackets;
    result.connCreated = connCreated - right.connCreated;
    result.connReleased = connReleased - right.connReleased;
    result.invalidPackets = invalidPackets - right.invalidPackets;
    return result;
}

NetworkAsio::NetworkAsio(std::shared_ptr<ServerEntry> server,
                         std::shared_ptr<NetworkMatrix> netMatrix,
                         std::shared_ptr<RequestMatrix> reqMatrix)
    :_connCreated(0),
     _server(server),
     _acceptCtx(std::make_unique<asio::io_context>()),
     _rwCtx(std::make_shared<asio::io_context>()),
     _acceptor(nullptr),
     _acceptThd(nullptr),
     _isRunning(false),
     _netMatrix(netMatrix),
     _reqMatrix(reqMatrix) {
}

std::unique_ptr<BlockingTcpClient> NetworkAsio::createBlockingClient(
        size_t readBuf) {
    INVARIANT(_rwCtx != nullptr);
    return std::move(std::make_unique<BlockingTcpClient>(_rwCtx, readBuf));
}

std::unique_ptr<BlockingTcpClient> NetworkAsio::createBlockingClient(
        asio::ip::tcp::socket socket, size_t readBuf) {
    INVARIANT(_rwCtx != nullptr);
    return std::move(std::make_unique<BlockingTcpClient>(
        _rwCtx, std::move(socket), readBuf));
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

Expected<uint64_t> NetworkAsio::client2Session(
                std::shared_ptr<BlockingTcpClient> c) {
    if (c->getReadBufSize() > 0) {
        return {ErrorCodes::ERR_NETWORK,
            "client still have buf unread, cannot transfer to a session"};
    }
    uint64_t connId = _connCreated.fetch_add(1, std::memory_order_relaxed);
    auto sess = std::make_shared<NetSession>(
                _server, std::move(c->borrowConn()),
                connId, true, _netMatrix, _reqMatrix);
    sess->getCtx()->setAuthed();
    _server->addSession(std::move(sess));
    ++_netMatrix->connCreated;
    return connId;
}

void NetworkAsio::doAccept() {
    auto cb = [this](const std::error_code& ec, tcp::socket socket) {
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
                std::make_shared<NetSession>(
                    _server, std::move(socket),
                    _connCreated.fetch_add(1, std::memory_order_relaxed),
                    true, _netMatrix, _reqMatrix)));
        ++_netMatrix->connCreated;
        doAccept();
    };
    _acceptor->async_accept(*_rwCtx, std::move(cb));
}

void NetworkAsio::stop() {
    LOG(INFO) << "network-asio begin stops...";
    _isRunning.store(false, std::memory_order_relaxed);
    _acceptCtx->stop();
    _rwCtx->stop();
    _acceptThd->join();
    for (auto& v : _rwThreads) {
        v.join();
    }
    LOG(INFO) << "network-asio stops complete...";
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

    size_t cpuNum = std::thread::hardware_concurrency();
    if (cpuNum == 0) {
        return {ErrorCodes::ERR_INTERNAL, "cpu num cannot be detected"};
    }
    for (size_t i = 0; i < std::max(size_t(4), cpuNum/4); ++i) {
        std::thread thd([this] {
            // TODO(deyukong): set threadname for debug/profile
            while (_isRunning.load(std::memory_order_relaxed)) {
                // if no workguard, the run() returns immediately if no tasks
                asio::io_context::work work(*_rwCtx);
                try {
                    _rwCtx->run();
                } catch (const std::exception& ex) {
                    LOG(FATAL) << "read/write thd failed:" << ex.what();
                } catch (...) {
                    LOG(FATAL) << "unknown exception";
                }
            }
        });
        _rwThreads.emplace_back(std::move(thd));
    }

    // TODO(deyukong): acceptor needs no explicitly listen.
    // but only through listen can we configure backlog.
    // _acceptor->listen(BACKLOG);
    doAccept();
    return {ErrorCodes::ERR_OK, ""};
}

NetSession::NetSession(std::shared_ptr<ServerEntry> server,
                       tcp::socket sock,
                       uint64_t connid,
                       bool initSock,
                       std::shared_ptr<NetworkMatrix> netMatrix,
                       std::shared_ptr<RequestMatrix> reqMatrix)
        :Session(server),
         _connId(connid),
         _closeAfterRsp(false),
         _state(State::Created),
         _sock(std::move(sock)),
         _queryBuf(std::vector<char>()),
         _queryBufPos(0),
         _reqType(RedisReqMode::REDIS_REQ_UNKNOWN),
         _multibulklen(0),
         _bulkLen(-1),
         _netMatrix(netMatrix),
         _reqMatrix(reqMatrix) {
    if (initSock) {
        std::error_code ec;
        _sock.non_blocking(true, ec);
        INVARIANT(ec.value() == 0);
        _sock.set_option(tcp::no_delay(true));
        _sock.set_option(asio::socket_base::keep_alive(true));
        // TODO(deyukong): keep-alive params
    }
}

void NetSession::setState(State s) {
    _state.store(s, std::memory_order_relaxed);
}

std::string NetSession::getRemoteRepr() const {
    if (_sock.is_open()) {
        return _sock.remote_endpoint().address().to_string();
    }
    return "closed conn";
}

std::string NetSession::getLocalRepr() const {
    if (_sock.is_open()) {
        return _sock.local_endpoint().address().to_string();
    }
    return "closed conn";
}

void NetSession::schedule() {
    // incr the reference, so it's safe to remove sessions
    // from _serverEntry at executing time.
    auto self(shared_from_this());
    _server->schedule([this, self]() {
        stepState();
    });
}

asio::ip::tcp::socket NetSession::borrowConn() {
    return std::move(_sock);
}

void NetSession::start() {
    stepState();
}

Status NetSession::cancel() {
    std::error_code ec;
    _sock.cancel(ec);
    if (!ec) {
        return {ErrorCodes::ERR_OK, ""};
    }
    return {ErrorCodes::ERR_NETWORK, ec.message()};
}

void NetSession::setArgs(const std::vector<std::string>& args) {
    _args = args;
}

const std::vector<std::string>& NetSession::getArgs() const {
    return _args;
}

void NetSession::setRspAndClose(const std::string& s) {
    _closeAfterRsp = true;

    setResponse(redis_port::errorReply(s));
    setState(State::DrainRsp);
    schedule();
}

void NetSession::processInlineBuffer() {
    char *newline = nullptr;
    std::vector<std::string> argv;
    std::string aux;
    size_t querylen;

    /* Search for end of line */
    newline = strchr(_queryBuf.data(), '\n');

    /* Nothing to do without a \r\n */
    if (newline == NULL) {
        if (_queryBufPos > REDIS_INLINE_MAX_SIZE) {
            ++_netMatrix->invalidPackets;
            setRspAndClose("Protocol error: too big inline request");
            return;
        }
        setState(State::DrainReqNet);
        schedule();
        return;
    }

    /* Handle the \r\n case. */
    if (newline && newline != _queryBuf.data() && *(newline-1) == '\r') {
        newline--;
    }

    /* Split the input buffer up to the \r\n */
    querylen = newline-(_queryBuf.data());
    aux = std::string(_queryBuf.data(), querylen);
    argv = redis_port::splitargs(aux);
    if (argv.size() == 0) {
        setRspAndClose("Protocol error: unbalanced quotes in request");
        return;
    }

    /* Leave data after the first line of the query in the buffer */
    shiftQueryBuf(querylen+2, -1);

    if (_args.size() != 0) {
        LOG(FATAL) << "BUG: _args.size:" << _args.size() << " not empty";
    }

    for (auto& v : argv) {
        if (v.length() != 0) {
            _args.emplace_back(v);
        }
    }

    setState(State::Process);
    schedule();
}

// NOTE(deyukong): mainly port from redis::networking.c,
// func:processMultibulkBuffer, the unportable part (long long, int and so on)
// are all from the redis source code, quite ugly.
// FIXME(deyukong): rewrite into a more c++ like code.
void NetSession::processMultibulkBuffer() {
    char *newLine = nullptr;
    long long ll;  // NOLINT(runtime/int)
    int pos = 0;
    int ok = 0;
    if (_multibulklen == 0) {
        newLine = strchr(_queryBuf.data(), '\r');
        if (newLine == nullptr) {
            if (_queryBufPos > REDIS_INLINE_MAX_SIZE) {
                ++_netMatrix->invalidPackets;
                setRspAndClose("Protocol error: too big mbulk count string");
                return;
            }
            // not complete line
            setState(State::DrainReqNet);
            schedule();
            return;
        }
        if (newLine - _queryBuf.data() > _queryBufPos - 2) {
            // not complete line
            setState(State::DrainReqNet);
            schedule();
            return;
        }
        if (_queryBuf[0] != '*') {
            LOG(FATAL) << "multiBulk first char not *";
            return;
        }
        char *newStart = _queryBuf.data() + 1;
        ok = redis_port::string2ll(newStart, newLine - newStart, &ll);
        if (!ok || ll > 1024*1024) {
            ++_netMatrix->invalidPackets;
            setRspAndClose("Protocol error: invalid multibulk length");
            return;
        }
        pos = newLine-_queryBuf.data()+2;
        if (ll <= 0) {
            shiftQueryBuf(pos, -1);
            if (_queryBufPos == 0) {
                setState(State::DrainReqNet);
            } else {
                setState(State::DrainReqBuf);
            }
            schedule();
            return;
        }
        _multibulklen = ll;
    }

    INVARIANT(_multibulklen > 0);

    while (_multibulklen) {
        if (_bulkLen == -1) {
            newLine = strchr(_queryBuf.data()+pos, '\r');
            if (newLine == nullptr) {
                if (_queryBufPos > REDIS_INLINE_MAX_SIZE) {
                    ++_netMatrix->invalidPackets;
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
                ++_netMatrix->invalidPackets;
                s << "Protocol error: expected '$', got '"
                  << _queryBuf.data()[pos] << "'";
                setRspAndClose(s.str());
                return;
            }
            char *newStart = _queryBuf.data()+pos+1;
            ok = redis_port::string2ll(newStart, newLine - newStart, &ll);
            if (!ok || ll < 0 || ll > 512*1024*1024) {
                ++_netMatrix->invalidPackets;
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
    } else {
        setState(State::DrainReqNet);
    }
    schedule();
}

void NetSession::drainReqCallback(const std::error_code& ec, size_t actualLen) {
    if (ec) {
        LOG(WARNING) << "drainReqCallback:" << ec.message();
        setState(State::End);
        schedule();
        return;
    }

    State curr = _state.load(std::memory_order_relaxed);
    INVARIANT(curr == State::DrainReqBuf || curr == State::DrainReqNet);

    _queryBufPos += actualLen;
    _queryBuf[_queryBufPos] = 0;
    if (_queryBufPos > REDIS_MAX_QUERYBUF_LEN) {
        ++_netMatrix->invalidPackets;
        setRspAndClose("Closing client that reached max query buffer length");
        return;
    }
    if (_reqType == RedisReqMode::REDIS_REQ_UNKNOWN) {
        if (_queryBuf[0] == '*') {
            _reqType = RedisReqMode::REDIS_REQ_MULTIBULK;
        } else {
            _reqType = RedisReqMode::REDIS_REQ_INLINE;
        }
    }
    if (_reqType == RedisReqMode::REDIS_REQ_MULTIBULK) {
        processMultibulkBuffer();
    } else if (_reqType == RedisReqMode::REDIS_REQ_INLINE) {
        processInlineBuffer();
    } else {
        LOG(FATAL) << "unknown request type";
    }
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

void NetSession::resetMultiBulkCtx() {
    _reqType = RedisReqMode::REDIS_REQ_UNKNOWN;
    _multibulklen = 0;
    _bulkLen = -1;
    _args.clear();
    _respBuf.clear();
}

void NetSession::drainReqBuf() {
    INVARIANT(_queryBufPos != 0);
    drainReqCallback(std::error_code(), 0);
}

void NetSession::drainReqNet() {
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
    auto self(shared_from_this());
    uint64_t curr = nsSinceEpoch();
    _sock.
        async_read_some(asio::buffer(_queryBuf.data() + _queryBufPos, wantLen),
        [this, self, curr](const std::error_code& ec, size_t actualLen) {
            _ctx->addReadPacketCost(nsSinceEpoch() - curr);
            drainReqCallback(ec, actualLen);
        });
}

void NetSession::processReq() {
    _ctx->setProcessPacketStart(nsSinceEpoch());
    bool continueSched = _server->processRequest(id());
    _ctx->setProcessPacketEnd(nsSinceEpoch());
    if (!continueSched) {
        _state.store(State::End);
        schedule();
    } else {
        _state.store(State::DrainRsp, std::memory_order_relaxed);
        schedule();
    }
}

void NetSession::drainRsp() {
    auto self(shared_from_this());
    _ctx->setSendPacketStart(nsSinceEpoch());
    asio::async_write(_sock, asio::buffer(_respBuf.data(), _respBuf.size()),
        [this, self](const std::error_code& ec, size_t actualLen) {
            _ctx->setSendPacketEnd(nsSinceEpoch());
            drainRspCallback(ec, actualLen);
    });
}

void NetSession::drainRspCallback(const std::error_code& ec, size_t actualLen) {
    if (ec) {
        LOG(WARNING) << "drainRspCallback:" << ec.message();
        setState(State::End);
        schedule();
        return;
    }
    if (actualLen != _respBuf.size()) {
        LOG(FATAL) << "conn:" << _connId << ",data:" << _respBuf.data()
            << ",actualLen:" << actualLen << ",bufsize:" << _respBuf.size()
            << ",invalid drainRsp len";
    }

    _reqMatrix->processed += 1;
    _reqMatrix->readPacketCost += _ctx->getReadPacketCost();
    _reqMatrix->processCost +=
            _ctx->getProcessPacketEnd() - _ctx->getProcessPacketStart();
    _reqMatrix->sendPacketCost +=
            _ctx->getSendPacketEnd() - _ctx->getSendPacketStart();
    _ctx->resetSingleReqCtx();

    if (_closeAfterRsp) {
        setState(State::End);
        schedule();
        return;
    }

    // clean reqMode and counters for multibulk mode
    resetMultiBulkCtx();
    if (_queryBufPos == 0) {
        setState(State::DrainReqNet);
    } else {
        setState(State::DrainReqBuf);
        ++_netMatrix->stickyPackets;
    }
    schedule();
}

void NetSession::endSession() {
    ++_netMatrix->connReleased;
    // NOTE(deyukong): endSession will call destructor
    // never write any codes after _server->endSession
    _server->endSession(id());
}

void NetSession::stepState() {
    if (_state.load(std::memory_order_relaxed) == State::Created) {
        INVARIANT(_reqType == RedisReqMode::REDIS_REQ_UNKNOWN);
        INVARIANT(_multibulklen == 0 && _bulkLen == -1);
        setState(State::DrainReqNet);
    }
    auto currState = _state.load(std::memory_order_relaxed);
    switch (currState) {
        case State::DrainReqNet:
            drainReqNet();
            return;
        case State::DrainReqBuf:
            drainReqBuf();
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
