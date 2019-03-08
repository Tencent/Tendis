// Copyright [2019] <eliotwang@tencent.com>
#include <chrono>

#include <memory>
#include <vector>
#include <utility>

#include "glog/logging.h"

#include "tendisplus/server/index_manager.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/portable.h"
#include "tendisplus/utils/string.h"

namespace tendisplus {

  IndexManager::IndexManager(std::shared_ptr<ServerEntry> svr)
        : _isRunning(false),
        _svr(svr),
        _scannerMatrix(std::make_shared<PoolMatrix>()),
        _deleterMatrix(std::make_shared<PoolMatrix>()),
        _totalDequeue(0),
        _totalEnqueue(0) {
          for (size_t storeId = 0; storeId < KVStore::INSTANCE_NUM; ++storeId) {
                _scanPoints[storeId] = std::move(std::string());
            }
        }

  Status IndexManager::startup() {
      Status s;

      _indexScanner = std::make_unique<WorkerPool>("index-scanner",
                                                   _scannerMatrix);
      s = _indexScanner->startup(1);
      if (!s.ok()) {
          return s;
      }

      _keyDeleter = std::make_unique<WorkerPool>("expired-key-deleter",
                                                 _deleterMatrix);
      s = _keyDeleter->startup(1);
      if (!s.ok()) {
          return s;
      }

      _isRunning.store(true, std::memory_order_relaxed);
      _runner = std::thread([this]() {
          run();
      });

      return {ErrorCodes::ERR_OK, ""};
  }

  Status IndexManager::scanExpiredKeysJob(uint32_t storeId) {
      LocalSessionGuard sg(_svr);
      auto expd = _svr->getSegmentMgr()->getDb(sg.getSession(),
                                               storeId,
                                               mgl::LockMode::LOCK_IS);
      if (!expd.ok()) {
          return expd.status();
      }

      auto ptxn = expd.value().store->createTransaction();
      if (!ptxn.ok()) {
          return ptxn.status();
      }

      std::unique_ptr<Transaction> txn = std::move(ptxn.value());
      auto cursor = txn->createTTLIndexCursor(msSinceEpoch());
      INVARIANT(_scanPoints.find(storeId) != _scanPoints.end());
      // seek to the place where we left NOTE: skip the entry
      // already push into list
      if (_scanPoints[storeId].size() > 0) {
          cursor->seek(_scanPoints[storeId]);
          if (!_scanPoints[storeId].compare(cursor->key().value())) {
              cursor->next();
          }
      }

      while (true) {
          auto record = cursor->next();
          if (!record.ok()) {
              // if no ttl index, or if ttl index not expired
              // scan again from _scanPoints[storeId] again
              // and we always resume from where we left, which
              // is remembered by _scanPoints[storeI]
              //
              // here's the invariant: if a ttl index T was picked
              // up by the scanner (which means its associate
              // key is expired), any attempt to inserting an ttl
              // index before T will result in a deletion of the
              // key.
              break;
          }

          _scanPoints[storeId].assign(record.value().encode());
          {
              std::lock_guard<std::mutex> lk(_mutex);
              _expiredKeys[storeId].push_back(std::move(record.value()));
              _totalEnqueue++;
              if (_expiredKeys[storeId].size() == 10000) {
                  break;
              }
          }

          TEST_SYNC_POINT_CALLBACK("InspectTotalEnqueue", &_totalEnqueue);
      }

      return {ErrorCodes::ERR_OK, ""};
  }

  bool IndexManager::tryDelExpiredKeysJob(uint32_t storeId) {
      int deletes = 0;

      while (true) {
          TTLIndex index;
          {
              std::lock_guard<std::mutex> lk(_mutex);
              if (_expiredKeys[storeId].empty()) {
                  break;
              }
              index = _expiredKeys[storeId].front();
          }

          LocalSessionGuard sg(_svr);
          auto sess = sg.getSession();
          sess->getCtx()->setAuthed();
          sess->getCtx()->setDbId(index.getDbId());
          Command::expireKeyIfNeeded(sg.getSession(),
                                     index.getPriKey(),
                                     index.getType());

          {
              std::lock_guard<std::mutex> lk(_mutex);
              INVARIANT(!_expiredKeys[storeId].empty());
              _expiredKeys[storeId].pop_front();
              _totalDequeue++;
              deletes++;
          }

          // break if delete a number of keys in the current store
          // TODO(eliotwang): make 1000 a config item
          if (deletes == 1000) {
              break;
          }

          TEST_SYNC_POINT_CALLBACK("InspectTotalDequeue", &_totalDequeue);
      }

      return deletes;
  }

  // call this in a forever loop
  Status IndexManager::run() {
      auto scheScanExpired = [this]() {
          for (uint32_t i = 0; i < KVStore::INSTANCE_NUM; ++i) {
              _indexScanner->schedule([this, i]() {
                  scanExpiredKeysJob(i);
              });
          }
      };

      auto schedDelExpired = [this]() {
          std::vector<uint32_t> stored_with_expires;

          {
              std::lock_guard<std::mutex> lk(_mutex);
              for (uint32_t i = 0; i < KVStore::INSTANCE_NUM; ++i) {
                  if (_expiredKeys[i].size() > 0) {
                      stored_with_expires.push_back(i);
                  }
              }
          }

          for (auto store_idx : stored_with_expires) {
              _keyDeleter->schedule([this, store_idx]() {
                  tryDelExpiredKeysJob(store_idx);
              });
          }

          return stored_with_expires.size() > 0;
      };

      TEST_SYNC_POINT_CALLBACK("BeforeIndexManagerLoop", &_isRunning);
      while (_isRunning.load(std::memory_order_relaxed)) {
          scheScanExpired();
          schedDelExpired();
          std::this_thread::sleep_for(std::chrono::seconds(10));
      }

      return {ErrorCodes::ERR_OK, ""};
  }

  void IndexManager::stop() {
      LOG(WARNING) << "index manager begins to stop...";
      _isRunning.store(false, std::memory_order_relaxed);
      _runner.join();
      _indexScanner->stop();
      _keyDeleter->stop();
      LOG(WARNING) << "index manager stopped...";
  }

  bool IndexManager::isRunning() {
      return _isRunning.load(std::memory_order_relaxed);
  }
}  // namespace tendisplus
