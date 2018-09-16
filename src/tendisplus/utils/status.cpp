#include <utility>
#include "tendisplus/utils/status.h"

namespace tendisplus {
Status::Status()
    :Status(ErrorCodes::ERR_OK, "") {
}

Status::Status(const ErrorCodes& code, const std::string& reason)
    :_errmsg(reason), _code(code) {
}

Status::Status(Status&& other)
    :_errmsg(std::move(other._errmsg)), _code(other._code) {
}

bool Status::ok() const {
    return _code == ErrorCodes::ERR_OK;
}

std::string Status::toString() const {
    return _errmsg;
}

ErrorCodes Status::code() const {
    return _code;
}

Status::~Status() {
}

}  // namespace tendisplus
