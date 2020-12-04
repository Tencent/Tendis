// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <utility>
#include <sstream>

#include "tendisplus/utils/status.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {
Status::Status() : Status(ErrorCodes::ERR_OK, "") {}

Status::Status(const ErrorCodes& code, const std::string& reason)
  : _errmsg(reason), _code(code) {}

Status::Status(Status&& other)
  : _errmsg(std::move(other._errmsg)), _code(other._code) {}

bool Status::ok() const {
  return _code == ErrorCodes::ERR_OK;
}

/*
shared.crlf = createObject(OBJ_STRING,sdsnew("\r\n"));
shared.ok = createObject(OBJ_STRING,sdsnew("+OK\r\n"));
shared.err = createObject(OBJ_STRING,sdsnew("-ERR\r\n"));
shared.emptybulk = createObject(OBJ_STRING,sdsnew("$0\r\n\r\n"));
shared.czero = createObject(OBJ_STRING,sdsnew(":0\r\n"));
shared.cone = createObject(OBJ_STRING,sdsnew(":1\r\n"));
shared.cnegone = createObject(OBJ_STRING,sdsnew(":-1\r\n"));
shared.nullbulk = createObject(OBJ_STRING,sdsnew("$-1\r\n"));
shared.nullmultibulk = createObject(OBJ_STRING,sdsnew("*-1\r\n"));
shared.emptymultibulk = createObject(OBJ_STRING,sdsnew("*0\r\n"));
shared.pong = createObject(OBJ_STRING,sdsnew("+PONG\r\n"));
shared.queued = createObject(OBJ_STRING,sdsnew("+QUEUED\r\n"));
shared.emptyscan = createObject(OBJ_STRING,sdsnew("*2\r\n$1\r\n0\r\n*0\r\n"));
shared.wrongtypeerr = createObject(OBJ_STRING,sdsnew(
"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"));
shared.nokeyerr = createObject(OBJ_STRING,sdsnew(
"-ERR no such key\r\n"));
shared.syntaxerr = createObject(OBJ_STRING,sdsnew(
"-ERR syntax error\r\n"));
shared.sameobjecterr = createObject(OBJ_STRING,sdsnew(
"-ERR source and destination objects are the same\r\n"));
shared.outofrangeerr = createObject(OBJ_STRING,sdsnew(
"-ERR index out of range\r\n"));
shared.noscripterr = createObject(OBJ_STRING,sdsnew(
"-NOSCRIPT No matching script. Please use EVAL.\r\n"));
shared.loadingerr = createObject(OBJ_STRING,sdsnew(
"-LOADING Redis is loading the dataset in memory\r\n"));
shared.slowscripterr = createObject(OBJ_STRING,sdsnew(
"-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN
NOSAVE.\r\n")); shared.masterdownerr = createObject(OBJ_STRING,sdsnew(
"-MASTERDOWN Link with MASTER is down and slave-serve-stale-data is set to
'no'.\r\n")); shared.bgsaveerr = createObject(OBJ_STRING,sdsnew(
"-MISCONF Redis is configured to save RDB snapshots, but it is currently not
able to persist on disk. Commands that may modify the data set are disabled,
because this instance is configured to report errors during writes if RDB
snapshotting fails (stop-writes-on-bgsave-error option). Please check the Redis
logs for details about the RDB error.\r\n")); shared.roslaveerr =
createObject(OBJ_STRING,sdsnew(
"-READONLY You can't write against a read only slave.\r\n"));
shared.noautherr = createObject(OBJ_STRING,sdsnew(
"-NOAUTH Authentication required.\r\n"));
shared.oomerr = createObject(OBJ_STRING,sdsnew(
"-OOM command not allowed when used memory > 'maxmemory'.\r\n"));
shared.execaborterr = createObject(OBJ_STRING,sdsnew(
"-EXECABORT Transaction discarded because of previous errors.\r\n"));
shared.noreplicaserr = createObject(OBJ_STRING,sdsnew(
"-NOREPLICAS Not enough good slaves to write.\r\n"));
shared.busykeyerr = createObject(OBJ_STRING,sdsnew(
"-BUSYKEY Target key name already exists.\r\n"));
shared.space = createObject(OBJ_STRING,sdsnew(" "));
shared.colon = createObject(OBJ_STRING,sdsnew(":"));
shared.plus = createObject(OBJ_STRING,sdsnew("+"));

*/

std::string Status::getErrStr(ErrorCodes code) {
  switch (code) {
    case ErrorCodes::ERR_NAN:
      return "-ERR resulting score is not a number (NaN)\r\n";
    case ErrorCodes::ERR_FLOAT:
      return "-ERR value is not a valid float\r\n";
    case ErrorCodes::ERR_INTERGER:
      return "-ERR value is not an integer or out of range\r\n";
    case ErrorCodes::ERR_PARSEOPT:
      return "-ERR syntax error\r\n";
    case ErrorCodes::ERR_ZSLPARSERANGE:
      return "-ERR min or max is not a float\r\n";
    case ErrorCodes::ERR_ZSLPARSELEXRANGE:
      return "-ERR min or max not valid string range item\r\n";
    case ErrorCodes::ERR_EXTENDED_PROTOCOL:
      return "-ERR invalid extended protocol input\r\n";
    case ErrorCodes::ERR_WRONG_TYPE:
      return "-WRONGTYPE Operation against a key holding the wrong kind "
             "of value\r\n";
    case ErrorCodes::ERR_WRONG_ARGS_SIZE:
      return "-ERR wrong number of arguments\r\n";
    case ErrorCodes::ERR_INVALID_HLL:
      return "-INVALIDOBJ Corrupted HLL object detected\r\n";
    case ErrorCodes::ERR_NO_KEY:
      return "-ERR no such key\r\n";
    case ErrorCodes::ERR_OUT_OF_RANGE:
      return "-ERR index out of range\r\n";
    case ErrorCodes::ERR_WRONG_VERSION_EP:
      return "-WRONGVERSION\r\n";
    case ErrorCodes::ERR_CLUSTER_REDIR_CROSS_SLOT:
      return "-CROSSSLOT Keys in request don't hash to the same slot\r\n";
    case ErrorCodes::ERR_CLUSTER_REDIR_DOWN_STATE:
      return "-CLUSTERDOWN The cluster is down\r\n";
    case ErrorCodes::ERR_CLUSTER_REDIR_DOWN_UNBOUND:
      return "-CLUSTERDOWN Hash slot not served\r\n";

    default:
      break;
  }

  return "";
}

std::string Status::toString() const {
  if (_errmsg.size() == 0) {
    return Status::getErrStr(_code);
  } else {
    std::stringstream ss;
    if (_code < ErrorCodes::ERR_AUTH) {
      ss << "-ERR:"
         << static_cast<std::underlying_type<ErrorCodes>::type>(_code)
         << ",msg:" << _errmsg << "\r\n";
    } else {
      // redis error
      if (_errmsg[0] == '-') {
        INVARIANT_D(_errmsg[_errmsg.size() - 2] == '\r');
        INVARIANT_D(_errmsg[_errmsg.size() - 1] == '\n');

        return _errmsg;
      } else {
        ss << "-ERR " << _errmsg << "\r\n";
      }
    }
    return ss.str();
  }
}

ErrorCodes Status::code() const {
  return _code;
}

Status::~Status() {}

const std::string& Status::getErrmsg() const {
  return _errmsg;
}

}  // namespace tendisplus
