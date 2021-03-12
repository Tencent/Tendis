// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <string>
#include <vector>
#include <algorithm>
#ifndef _WIN32
#include <experimental/optional>
#endif
#include "tendisplus/storage/varint.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/storage/skiplist.h"

namespace tendisplus {
constexpr uint64_t MAXSEQ = 9223372036854775807ULL;
constexpr uint64_t INITSEQ = MAXSEQ / 2ULL;

class SortCommand : public Command {
 private:
  struct SortOp {
    mystring_view cmd;
    mystring_view pattern;
    std::vector<std::string> priKey;
    std::string field;
  };

  struct Element {
    std::string key;
    double score;
    std::string sortBy;
    size_t uniqueId;
  };

  std::pair<std::string, std::string> parsePattern(const std::string& key,
                                                   mystring_view pattern) {
    if (pattern.size() == 0) {
      return std::make_pair("", "");
    }
    if (pattern[0] == '#' && pattern.size() == 1) {
      return std::make_pair(key, "");
    }
    size_t starPos = pattern.find('*');
    if (starPos == decltype(pattern)::npos) {
      return std::make_pair("", "");
    }
    auto hashpos = pattern.find("->");
    bool isHash(false);
    if (hashpos != decltype(pattern)::npos && hashpos < pattern.size() - 2) {
      isHash = true;
    }
    if (isHash && starPos > hashpos) {
      return std::make_pair("", "");
    }

    std::string combine(pattern.data(), starPos);
    std::string field;
    combine += key;
    if (pattern.begin() + starPos + 1 < pattern.end()) {
      if (isHash) {
        combine.append(pattern.begin() + starPos + 1,
                       pattern.begin() + hashpos);
        field.append(pattern.begin() + hashpos + 2, pattern.end());
      } else {
        combine.append(pattern.begin() + starPos + 1, pattern.end());
      }
    }
    return std::make_pair(std::move(combine), std::move(field));
  }

  Expected<std::string> getPatternResult(Session* sess,
                                         const std::string& metaKey,
                                         const std::string& fieldKey) {
    const auto& server = sess->getServerEntry();
    const auto& pCtx = sess->getCtx();
    auto expdb = server->getSegmentMgr()->getDbHasLocked(sess, metaKey);
    if (!expdb.ok()) {
      return expdb.status();
    }

    auto byRv =
      Command::expireKeyIfNeeded(sess, metaKey, RecordType::RT_DATA_META);
    // should handle NOT_FOUND and EXPIRED outsie
    if (!byRv.ok()) {
      return byRv.status();
    }
    auto byStore = expdb.value().store;
    auto byExptxn = sess->getCtx()->createTransaction(byStore);
    if (!byExptxn.ok()) {
      return byExptxn.status();
    }

    if (fieldKey.size() != 0) {
      RecordKey hashRk(expdb.value().chunkId,
                       pCtx->getDbId(),
                       RecordType::RT_HASH_ELE,
                       metaKey,
                       fieldKey);
      auto hashVal = byStore->getKV(hashRk, byExptxn.value());
      if (!hashVal.ok()) {
        return hashVal.status();
      }
      return std::move(hashVal.value().getValue());
    } else {
      RecordKey kvRk(
        expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_KV, metaKey, "");
      auto kvVal = byStore->getKV(kvRk, byExptxn.value());
      if (!kvVal.ok()) {
        return kvVal.status();
      }
      return std::move(kvVal.value().getValue());
    }
  }


 public:
  SortCommand() : Command("sort", "wm") {}

  ssize_t arity() const {
    return -2;
  }

  int32_t firstkey() const {
    return 1;
  }

  int32_t lastkey() const {
    return 1;
  }

  int32_t keystep() const {
    return 1;
  }

  std::vector<int> getKeysFromCommand(const std::vector<std::string>& argv) {
    uint32_t i, j;
    std::vector<int> keys;

    keys.push_back(1); /* <sort-key> is always present. */

    /* Search for STORE option. By default we consider options to don't
     * have arguments, so if we find an unknown option name we scan the
     * next. However there are options with 1 or 2 arguments, so we
     * provide a list here in order to skip the right number of args. */
    struct {
      std::string name;
      int skip;
    } skiplist[] = {
      {"limit", 2}, {"get", 1}, {"by", 1}, {"", 0} /* End of elements. */
    };

    for (i = 2; i < argv.size(); i++) {
      for (j = 0; skiplist[j].name != ""; j++) {
        if (toLower(argv[i]) == skiplist[j].name) {
          i += skiplist[j].skip;
          break;
        } else if (toLower(argv[i]) == "store" && i + 1 < argv.size()) {
          keys.push_back(i + 1); /* <store-key> */
          break;
        }
      }
    }
    return keys;
  }

  Expected<std::string> run(Session* sess) final {
    const auto& args = sess->getArgs();
    const auto& key = args[1];
    bool desc(false), alpha(false), store(false), nosort(false), sortby(false),
      exist(true);
    int32_t offset(-1), count(-1);
    size_t storeKeyIndex;

    /* 1.precheck */
    std::vector<SortOp> ops(1);
    std::vector<int> iKey(1, 1);
    for (size_t i = 2; i < args.size(); i++) {
      size_t leftargs = args.size() - i - 1;
      if (!::strcasecmp(args[i].c_str(), "asc")) {
        desc = false;
      } else if (!::strcasecmp(args[i].c_str(), "desc")) {
        desc = true;
      } else if (!::strcasecmp(args[i].c_str(), "alpha")) {
        alpha = true;
      } else if (!::strcasecmp(args[i].c_str(), "limit") && leftargs >= 2) {
        auto eOffset = tendisplus::stol(args[++i]);
        if (!eOffset.ok()) {
          return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
        }
        offset = eOffset.value();
        auto eCount = tendisplus::stol(args[++i]);
        if (!eCount.ok()) {
          return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
        }
        count = eCount.value();
      } else if (!::strcasecmp(args[i].c_str(), "store")) {
        store = true;
        storeKeyIndex = i + 1;
        i++;
      } else if (!::strcasecmp(args[i].c_str(), "by") && leftargs >= 1) {
        sortby = true;
        ops[0].cmd = "by";
        ops[0].pattern = mystring_view(args[i + 1].c_str(), args[i + 1].size());
        i++;
        if (ops[0].pattern.find('*') == decltype(ops[0].pattern)::npos) {
          nosort = true;
        } else {
          /* If BY is specified with a real patter, we can't accept
           * it in cluster mode. */
          if (sess->getServerEntry()->isClusterEnabled()) {
            return {ErrorCodes::ERR_PARSEOPT,
                    "BY option of SORT denied in Cluster mode."};
          }
        }
      } else if (!::strcasecmp(args[i].c_str(), "get") && leftargs >= 1) {
        if (sess->getServerEntry()->isClusterEnabled()) {
          return {ErrorCodes::ERR_PARSEOPT,
                  "GET option of SORT denied in Cluster mode."};
        }
        mystring_view cmd("get");
        ops.emplace_back(SortOp{
          "get",
          {args[i + 1].c_str(), args[i + 1].size()},
        });
        iKey.push_back(i + 1);
        i++;
      } else {
        return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
      }
    }

    /* 2. lock all keys expect [Get pattern] */
    auto server = sess->getServerEntry();
    auto pCtx = sess->getCtx();
    auto keyidx = getKeysFromCommand(sess->getArgs());
    auto expdblist = server->getSegmentMgr()->getAllKeysLocked(
      sess,
      sess->getArgs(),
      keyidx,
      store ? mgl::LockMode::LOCK_X : Command::RdLock());
    RET_IF_ERR_EXPECTED(expdblist);

    auto expdb = server->getSegmentMgr()->getDbHasLocked(sess, key);
    RET_IF_ERR_EXPECTED(expdb);
    PStore kvstore = expdb.value().store;

    /* 3. Get the sort key and length */
    auto expRv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_DATA_META);
    if (expRv.status().code() == ErrorCodes::ERR_EXPIRED ||
        expRv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      exist = false;
    } else if (!expRv.ok()) {
      return expRv.status();
    }

    RecordKey metaRk(expdb.value().chunkId,
                     pCtx->getDbId(),
                     RecordType::RT_ZSET_META,
                     key,
                     "");
    std::unique_ptr<RecordValue> rv;

    if (!exist) {
      ListMetaValue lm(INITSEQ, INITSEQ);
      rv = std::make_unique<RecordValue>(
        lm.encode(), RecordType::RT_LIST_META, sess->getCtx()->getVersionEP());
    } else {
      rv = std::make_unique<RecordValue>(expRv.value());
    }
    auto keyType = rv->getRecordType();
    if (keyType != RecordType::RT_LIST_META &&
        keyType != RecordType::RT_SET_META &&
        keyType != RecordType::RT_ZSET_META) {
      return {ErrorCodes::ERR_WRONG_TYPE, ""};
    }

    /* When sorting a set with no sort specified, we must sort the output
     * so the result is consistent across scripting and replication.
     *
     * The other types (list, sorted set) will retain their native order
     * even if no sort order is requested, so they remain stable across
     * scripting and replication.
     * there should not be any lua scripts invoked directly. */
    if (nosort && keyType == RecordType::RT_SET_META && store) {
      nosort = 0;
      alpha = 1;
      ops[0] = SortOp{};
      sortby = false;
    }

    // get the length of the object
    ssize_t veclen(0);
    uint64_t lHead(0), lTail(0);
    std::unique_ptr<SkipList> sl(nullptr);
    switch (keyType) {
      case RecordType::RT_LIST_META: {
        auto lm = ListMetaValue::decode(rv->getValue());
        if (!lm.ok()) {
          return lm.status();
        }
        lHead = lm.value().getHead();
        lTail = lm.value().getTail();
        veclen = lTail - lHead;
        break;
      }
      case RecordType::RT_SET_META: {
        auto sm = SetMetaValue::decode(rv->getValue());
        if (!sm.ok()) {
          return sm.status();
        }
        veclen = sm.value().getCount();
        break;
      }
      case RecordType::RT_ZSET_META: {
        auto zm = ZSlMetaValue::decode(rv->getValue());
        if (!zm.ok()) {
          return zm.status();
        }
        ZSlMetaValue meta = zm.value();
        veclen = meta.getCount() - 1;
        sl = std::make_unique<SkipList>(metaRk.getChunkId(),
                                        metaRk.getDbId(),
                                        metaRk.getPrimaryKey(),
                                        meta,
                                        kvstore);
        break;
      }
      default:
        INVARIANT_D(0);
        break;
    }

    ssize_t start(0), end(veclen > 0 ? veclen - 1 : 0);
    if (offset >= 0) {
      start = static_cast<ssize_t>(offset);
    }
    if (count >= 0) {
      end = start + static_cast<ssize_t>(count) - 1;
    }
    if (start >= veclen) {
      start = veclen - 1;
      end = veclen - 2;
    }
    if (end >= veclen) {
      end = veclen - 1;
    }

    if ((keyType == RecordType::RT_ZSET_META ||
         keyType == RecordType::RT_LIST_META) &&
        nosort && (start != 0 || end != veclen)) {
      veclen = end - start + 1;
    }
    std::vector<Element> records;
    records.reserve(veclen);

    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    if (keyType == RecordType::RT_LIST_META) {
      uint64_t pos(0), stop(0);
      int32_t sign = 1;

      if (nosort) {
        pos = lHead + start;
        stop = lHead + end;
        if (desc) {
          pos = lTail - start;
          stop = lTail - end;
          sign = -1;
        }
      } else {
        pos = lHead;
        stop = lTail - 1;
        if (desc) {
          pos = lTail - 1;
          stop = lHead;
          sign = -1;
        }
      }

      while (sign * static_cast<int64_t>(stop - pos) >= 0) {
        RecordKey subRk(expdb.value().chunkId,
                        pCtx->getDbId(),
                        RecordType::RT_LIST_ELE,
                        key,
                        std::to_string(pos));
        Expected<RecordValue> expRv = kvstore->getKV(subRk, ptxn.value());
        if (!expRv.ok()) {
          return expRv.status();
        }
        records.emplace_back(Element{expRv.value().getValue(), 0});
        pos += sign;
      }
    } else if (keyType == RecordType::RT_SET_META) {
      auto cursor = ptxn.value()->createDataCursor();
      RecordKey fakeRk = {expdb.value().chunkId,
                          pCtx->getDbId(),
                          RecordType::RT_SET_ELE,
                          key,
                          ""};
      cursor->seek(fakeRk.prefixPk());
      while (true) {
        Expected<Record> expRcd = cursor->next();
        if (expRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
          break;
        }
        if (!expRcd.ok()) {
          return expRcd.status();
        }
        Record& rcd = expRcd.value();
        const RecordKey& rcdkey = rcd.getRecordKey();
        if (rcdkey.prefixPk() != fakeRk.prefixPk()) {
          break;
        }
        records.emplace_back(Element{rcdkey.getSecondaryKey(), 0});
      }
    } else if (keyType == RecordType::RT_ZSET_META) {
      int64_t pos(0), rangeLen(sl->getCount() - 1);
      if (nosort) {
        pos = start;
        rangeLen = veclen;
      }
      auto arr = sl->scanByRank(pos, rangeLen, desc, ptxn.value());
      if (!arr.ok()) {
        return arr.status();
      }
      for (auto& x : arr.value()) {
        records.emplace_back(Element{x.second, 0});
      }
    } else {
      INVARIANT_D(0);
    }

    /* 4. Handle by pattern|get pattern, get opkeys  */
    std::vector<std::string> opkeys;
    std::vector<int32_t> opidx;
    size_t lockidx(0);
    for (size_t i = 0; i < ops.size(); i++) {
      ops[i].priKey.resize(records.size());
      for (size_t j = 0; j < records.size(); j++) {
        records[j].uniqueId = j;
        auto keyPair = parsePattern(records[j].key, ops[i].pattern);
        const auto& pri = keyPair.first;
        if (pri.size() == 0) {
          continue;
        }
        ops[i].priKey[j] = keyPair.first;  // copy
        opkeys.emplace_back(std::move(keyPair.first));
        opidx.push_back(lockidx++);

        // if "field" exists, each of them should has exact the same
        // field indeed. so just move it once.
        if (ops[i].field.size() == 0 && keyPair.second.size() != 0) {
          ops[i].field = std::move(keyPair.second);
        }
      }
    }

    /* It disable get pattern|by pattern when cluster_enabled is yes */
    INVARIANT_D(!server->isClusterEnabled() || opkeys.size() == 0);
    // handle GET
    std::vector<std::string> result;
    {
      /* When cluster_enabled is yes, opkeys is always empty. It is only
       * compatible with when cluster_enable is no */
      auto locklist = server->getSegmentMgr()->getAllKeysLocked(
        sess, opkeys, opidx, Command::RdLock());

      if (!nosort) {
        const auto& op = ops[0];
        const auto& priKeylist = op.priKey;
        std::string nval;
        for (size_t i = 0; i < records.size(); i++) {
          auto& ele = records[i];
          if (!sortby) {
            nval = ele.key;
          } else {
            // handle * not found
            if (priKeylist[i].size() == 0) {
              continue;
            }
            // handle "BY #"
            if (priKeylist[i] == ele.key) {
              // set subkey itself as value.
              nval = ele.key;
            } else {
              const auto& field = op.field;
              auto expVal = getPatternResult(sess, priKeylist[i], field);
              if (expVal.status().code() == ErrorCodes::ERR_NOTFOUND ||
                  expVal.status().code() == ErrorCodes::ERR_EXPIRED) {
                continue;
              }
              if (!expVal.ok()) {
                return expVal.status();
              }
              nval = std::move(expVal.value());
            }
          }

          // if alpha set, nkey
          if (alpha) {
            ele.sortBy = std::move(nval);
          } else {
            auto eScore = tendisplus::stod(nval);
            if (!eScore.ok()) {
              return {ErrorCodes::ERR_WRONG_TYPE,
                      "One or more scores can't be converted "
                      "into double"};
            }
            records[i].score = eScore.value();
          }
        }
      }

      if (!nosort) {
        std::sort(records.begin(),
                  records.end(),
                  [&sortby, &desc, &alpha](const Element& a, const Element& b) {
                    bool ret(true);
                    if (alpha) {
                      if (sortby) {
                        ret = a.sortBy < b.sortBy;
                      } else {
                        ret = a.key < b.key;
                      }
                    } else {
                      if (a.score == b.score) {
                        ret = a.key < b.key;
                      } else {
                        ret = a.score < b.score;
                      }
                    }
                    return desc ? !ret : ret;
                  });
      }

      // ops has a minimum size 1.
      result.reserve(ops.size() > 1 ? (ops.size() - 1) * records.size()
                                    : records.size());  // NOLINT
      if (ops.size() == 1) {
        ops.emplace_back(SortOp{"", ""});
      }
      ssize_t sortStart(0), sortEnd(records.size());
      if (start != 0 || end != 0) {
        sortStart = start;
        sortEnd = end + 1;
      }
      for (ssize_t i = sortStart; i < sortEnd; i++) {
        for (size_t j = 1; j < ops.size(); j++) {
          const auto& op = ops[j];
          size_t uniqueId = records[i].uniqueId;
          if (op.cmd == "") {
            result.emplace_back(records[i].key);
            continue;
          }
          const auto& priKeylist = op.priKey;
          if (priKeylist[uniqueId].size() == 0) {
            // nullBulk.
            result.emplace_back("");
          } else if (priKeylist[uniqueId] == records[i].key) {
            result.emplace_back(records[i].key);
          } else {
            const auto& field = op.field;
            auto expVal = getPatternResult(sess, priKeylist[uniqueId], field);
            if (expVal.status().code() == ErrorCodes::ERR_NOTFOUND ||
                expVal.status().code() == ErrorCodes::ERR_EXPIRED) {
              result.emplace_back("");
            }
            if (!expVal.ok()) {
              return expVal.status();
            }
            result.emplace_back(expVal.value());
          }
        }
      }
    }

    /* 5. Handle store */
    if (store) {
      auto addDb =
        server->getSegmentMgr()->getDbHasLocked(sess, args[storeKeyIndex]);
      if (!addDb.ok()) {
        return addDb.status();
      }
      auto addStore = addDb.value().store;
      auto addPtxn = sess->getCtx()->createTransaction(addStore);
      if (!addPtxn.ok()) {
        return addPtxn.status();
      }

      auto expDone = Command::delKeyChkExpire(
        sess, args[storeKeyIndex], RecordType::RT_DATA_META, addPtxn.value());
      if (!expDone.ok()) {
        return expDone.status();
      }
      if (result.size() == 0) {
        auto expCmt = sess->getCtx()->commitTransaction(addPtxn.value());
        if (!expCmt.ok()) {
          return expCmt.status();
        }
        return Command::fmtZero();
      }

      RecordKey metaRk(addDb.value().chunkId,
                       pCtx->getDbId(),
                       RecordType::RT_LIST_META,
                       args[storeKeyIndex],
                       "");
      ListMetaValue lm(INITSEQ, INITSEQ);
      uint64_t head = lm.getHead();
      uint64_t idx = head++;
      for (const auto& x : result) {
        RecordKey subRk(metaRk.getChunkId(),
                        metaRk.getDbId(),
                        RecordType::RT_LIST_ELE,
                        metaRk.getPrimaryKey(),
                        std::to_string(idx++));
        RecordValue subRv(x, RecordType::RT_LIST_ELE, -1);
        Status s = addStore->setKV(subRk, subRv, addPtxn.value());
        if (!s.ok()) {
          return s;
        }
      }
      lm.setTail(idx);
      Status s = addStore->setKV(metaRk,
                                 RecordValue(lm.encode(),
                                             RecordType::RT_LIST_META,
                                             pCtx->getVersionEP()),
                                 addPtxn.value());
      if (!s.ok()) {
        return s;
      }

      auto expCmt = sess->getCtx()->commitTransaction(addPtxn.value());
      if (!expCmt.ok()) {
        return expCmt.status();
      }
      return Command::fmtLongLong(result.size());
    }

    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, result.size());
    for (size_t i = 0; i < result.size(); i++) {
      Command::fmtBulk(ss, result[i]);
    }

    return ss.str();
  }
} sortCmd;

}  // namespace tendisplus
