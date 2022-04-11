// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "glog/logging.h"
#include "tendisplus/cluster/migrate_manager.h"
#include "tendisplus/cluster/migrate_batch.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

MigrateBatch::~MigrateBatch() {
  // NOTE(takenliu): if sender task is break, _buffer will have some data
  // INVARIANT_D(_buffer.size() == 0);
}

Status MigrateBatch::add(const std::string& key, const std::string& value) {
  uint32_t keylen = key.size();
  easyCopy(&_buffer, &_addBytes, keylen);
  easyCopy(&_buffer, &_addBytes, key.c_str(), keylen);

  uint32_t valuelen = value.size();
  easyCopy(&_buffer, &_addBytes, valuelen);
  easyCopy(&_buffer, &_addBytes, value.c_str(), valuelen);

  _counter++;
  INVARIANT_D(_addBytes == _buffer.size());
  return {ErrorCodes::ERR_OK, ""};
}

Status MigrateBatch::send() {
  Status s;
  if (_addBytes > 0) {
    SyncWriteData("5");
    SyncWriteData(
      string(reinterpret_cast<char*>(&_addBytes), sizeof(uint32_t)));
    SyncWriteData(string(_buffer.begin(), _buffer.end()));
    uint32_t sendBytes = _buffer.size() + 1 + sizeof(uint32_t);

    // Rate limit for migration
    _svr->getMigrateManager()->requestRateLimit(sendBytes);

    INVARIANT_D(_addBytes == _buffer.size());
    _sentBytes += sendBytes;

    _buffer.clear();
    _addBytes = 0;
    _sendCounter++;
  }

  return {ErrorCodes::ERR_OK, ""};
}

bool MigrateBatch::isFull() const {
  return _addBytes >= _maxBytes;
}

bool MigrateBatch::isEmpty() const {
  return _addBytes > 0;
}

uint32_t MigrateBatch::sendBytes() const {
  return _sentBytes;
}

uint32_t MigrateBatch::sendKVEntries() const {
  return _counter;
}

}  // namespace tendisplus
