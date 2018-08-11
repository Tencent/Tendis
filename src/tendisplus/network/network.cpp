#include <iostream>
#include <memory>
#include <string>
#include "tendisplus/network/network.h"

namespace tendisplus {

using asio::ip::tcp;

NetworkAsio::NetworkAsio()
    :_acceptCtx(std::make_unique<asio::io_context>()),
     _workCtx(std::make_unique<asio::io_context>()),
     _acceptor(nullptr),
     _acceptThd(nullptr),
     _isRunning(false) {
}

Status NetworkAsio::prepare(const std::string& ip, const uint16_t port) {
    asio::ip::address address = asio::ip::make_address(ip);
    auto ep = tcp::endpoint(address, port);

    std::error_code ec;
    _acceptor = std::make_unique<tcp::acceptor>(*_acceptCtx, ep);
    _acceptor->open(ep.protocol());
    _acceptor->set_option(tcp::acceptor::reuse_address(true));
    _acceptor->non_blocking(true, ec);
    if (ec) {
        return {ErrorCodes::ERR_NETWORK, ec.message()};
    }
    _acceptor->bind(ep, ec);
    if (ec) {
        return {ErrorCodes::ERR_NETWORK, ec.message()};
    }
    return {ErrorCodes::ERR_OK, ""};
}

void NetworkAsio::doAccept() {
    _acceptor->async_accept([this](std::error_code ec, tcp::socket socket) {
        if (!_isRunning.load(std::memory_order_relaxed)) {
            std::cout << "acceptCb, server is shuting down" << std::endl;
            return;
        }
        if (ec) {
            std::cout << "acceptCb errorcode:" << ec.message() << std::endl;
            // we log this error, but dont return
        }

        // TODO(deyukong): enqueue
        doAccept();
    });
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
                std::cerr<< "accept failed:" << ex.what() << std::endl;
                assert(0);
            } catch (...) {
                std::cerr<< "unknown exception" << std::endl;
                assert(0);
            }
        }
    });

    // TODO(deyukong): acceptor needs no explicitly listen.
    // but only through listen can we configure backlog.
    // _acceptor->listen(BACKLOG);
    doAccept();
    return {ErrorCodes::ERR_OK, ""};
}

}  // namespace tendisplus
