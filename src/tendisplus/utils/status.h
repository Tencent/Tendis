#ifndef SRC_TENDISPLUS_UTILS_STATUS_H_
#define SRC_TENDISPLUS_UTILS_STATUS_H_

#include <stdlib.h>
#include <utility>
#include <string>
#include <memory>
#include <execinfo.h>
#include <type_traits>
#include "tendisplus/utils/portable.h"

namespace tendisplus {

enum class ErrorCodes {
    ERR_OK,
    ERR_NETWORK,
    ERR_TIMEOUT,
    ERR_INTERNAL,
    ERR_PARSEOPT,
    ERR_PARSEPKT,
    ERR_COMMIT_RETRY,
    ERR_NOTFOUND,
    ERR_DECODE,
    ERR_AUTH,
    ERR_BUSY,
    ERR_EXHAUST,  // for cursor
    ERR_EXPIRED,
};

class Status {
 public:
    Status();
    Status(const ErrorCodes& code, const std::string& reason);
    Status(const Status& other) = default;
    Status(Status&& other);
    Status& operator=(const Status& other) = default;
    ~Status();
    bool ok() const;
    std::string toString() const;
    ErrorCodes code() const;
 private:
    std::string _errmsg;
    ErrorCodes _code;
};  // Status

// a practical impl of expected monad
// a better(than try/catch) way to handle error-returning
// see https://meetingcpp.com/2017/talks/items/Introduction_to_proposed_std__expected_T__E_.html
template<typename T>
class Expected {
 public:
    static_assert(!std::is_same<T, Status>::value,
        "invalid use of recursive Expected<Status>");

    Expected(ErrorCodes code, const std::string& reason)
        :_status(Status(code, reason)) {
    }

    // here we ignore "explicit" to make return two types
    // Status/T possible. It's more convinent to use
    Expected(const Status& other)  // NOLINT(runtime/explicit)
            :_status(other) {
        if (_status.ok()) {
            static const char *s =
                "can not use OK as Expected input"
                ", this makes data field empty,"
                ", which is always a misuse\n";
            std::stringstream ss;
            void *buffer[100];
            char **strings;
            int j, nptrs;
            nptrs = backtrace(buffer, 100);
            strings = backtrace_symbols(buffer, nptrs);
            ss << s;
            for (j = 0; j < nptrs; j++) {
                ss << strings[j] << "\n";
            }
            free(strings);
            throw std::invalid_argument(ss.str());
        }
    }

    Expected(T t)  // NOLINT(runtime/explicit)
        :_data(std::move(t)), _status(Status(ErrorCodes::ERR_OK, "")) {
    }

    const T& value() const {
        return *_data;
    }

    T& value() {
        return *_data;
    }

    const Status& status() const {
        return _status;
    }

    bool ok() const {
        return _status.ok();
    }

 private:
    optional<T> _data;
    Status _status;
};

// it is similar to std::make_unique
template <typename T, typename... Args>
Expected<T> makeExpected(Args&&... args) {
    return Expected<T>{T(std::forward<Args>(args)...)};
}

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_UTILS_STATUS_H_
