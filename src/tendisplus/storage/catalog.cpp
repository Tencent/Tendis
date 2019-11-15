#include <sstream>
#include <utility>
#include <memory>
#include <string>
#include "glog/logging.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h"
#include "tendisplus/storage/catalog.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {

StoreMeta::StoreMeta()
    :StoreMeta(0, "", 0, -1, 0, ReplState::REPL_NONE) {
}

StoreMeta::StoreMeta(uint32_t id_, const std::string& syncFromHost_,
                uint16_t syncFromPort_, int32_t syncFromId_,
                uint64_t binlogId_, ReplState replState_)
    :id(id_),
     syncFromHost(syncFromHost_),
     syncFromPort(syncFromPort_),
     syncFromId(syncFromId_),
     binlogId(binlogId_),
     replState(replState_) {
}

std::unique_ptr<StoreMeta> StoreMeta::copy() const {
    return std::move(std::unique_ptr<StoreMeta>(
        new StoreMeta(*this)));
}



ChunkMeta::ChunkMeta()
    :ChunkMeta(0, "", 0, -1, 0, MigrateReceiveState::NONE) {
}

ChunkMeta::ChunkMeta(uint32_t id_, const std::string& syncFromHost_,
                uint16_t syncFromPort_, int32_t syncFromId_,
                uint64_t binlogId_, MigrateReceiveState migrateState_)
    :id(id_),
     syncFromHost(syncFromHost_),
     syncFromPort(syncFromPort_),
     syncFromId(syncFromId_),
     binlogId(binlogId_),
     migrateState(migrateState_) {
}

std::unique_ptr<ChunkMeta> ChunkMeta::copy() const {
    return std::move(std::unique_ptr<ChunkMeta>(
        new ChunkMeta(*this)));
}



std::unique_ptr<StoreMainMeta> StoreMainMeta::copy() const {
    return std::move(std::unique_ptr<StoreMainMeta>(
        new StoreMainMeta(*this)));
}

std::unique_ptr<MainMeta> MainMeta::copy() const {
    return std::move(std::unique_ptr<MainMeta>(
        new MainMeta(*this)));
}

Catalog::Catalog(std::unique_ptr<KVStore> store,
        uint32_t kvStoreCount, uint32_t chunkSize)
    :_store(std::move(store)),
    _kvStoreCount(kvStoreCount),
    _chunkSize(chunkSize) {
    auto mainMeta = getMainMeta();
    if (mainMeta.ok()) {
        if (_kvStoreCount != mainMeta.value()->kvStoreCount ||
            _chunkSize != mainMeta.value()->chunkSize) {
            LOG(FATAL) << "kvStoreCount(" << _kvStoreCount  << ","
                << mainMeta.value()->kvStoreCount
                << ") or chunkSize(" << _chunkSize << ","
                << mainMeta.value()->chunkSize
                << ") not equal";
            INVARIANT(0);
        }
    } else if (mainMeta.status().code() == ErrorCodes::ERR_NOTFOUND) {
        auto pMeta = std::unique_ptr<MainMeta>(
            new MainMeta(kvStoreCount, chunkSize));
        Status s = setMainMeta(*pMeta);
        if (!s.ok()) {
            LOG(FATAL) << "catalog setMainMeta error:"
                << s.toString();
            INVARIANT(0);
        }
    } else {
       LOG(FATAL) << "catalog getMainMeta error:"
                << mainMeta.status().toString();
            INVARIANT(0);
    }
}

Status Catalog::setStoreMeta(const StoreMeta& meta) {
    std::stringstream ss;
    ss << "store_" << meta.id;
    RecordKey rk(0, 0, RecordType::RT_META, ss.str(), "");
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    writer.StartObject();

    writer.Key("Version");
    writer.String("1");

    writer.Key("syncFromHost");
    writer.String(meta.syncFromHost);

    writer.Key("syncFromPort");
    writer.Uint64(meta.syncFromPort);

    writer.Key("syncFromId");
    writer.Uint64(meta.syncFromId);

    writer.Key("binlogId");
    writer.Uint64(meta.binlogId);

    writer.Key("replState");
    writer.Uint64(static_cast<uint8_t>(meta.replState));

    writer.Key("id");
    writer.Uint64(meta.id);

    writer.EndObject();

    RecordValue rv(sb.GetString(), RecordType::RT_META, -1);

    auto exptxn = _store->createTransaction(nullptr);
    if (!exptxn.ok()) {
        return exptxn.status();
    }

    Transaction *txn = exptxn.value().get();

    Record rd(std::move(rk), std::move(rv));
    Status s = _store->setKV(rd, txn);
    if (!s.ok()) {
        return s;
    }
    return txn->commit().status();
}

Expected<std::unique_ptr<StoreMeta>> Catalog::getStoreMeta(uint32_t idx) {
    auto result = std::make_unique<StoreMeta>();
    std::stringstream ss;
    ss << "store_" << idx;
    RecordKey rk(0, 0, RecordType::RT_META, ss.str(), "");

    auto exptxn = _store->createTransaction(nullptr);
    if (!exptxn.ok()) {
        return exptxn.status();
    }

    Transaction *txn = exptxn.value().get();
    Expected<RecordValue> exprv = _store->getKV(rk, txn);
    if (!exprv.ok()) {
        return exprv.status();
    }
    RecordValue rv = exprv.value();
    const std::string& json = rv.getValue();

    rapidjson::Document doc;
    doc.Parse(json);
    if (doc.HasParseError()) {
        LOG(FATAL) << "parse meta failed"
            << rapidjson::GetParseError_En(doc.GetParseError());
    }

    INVARIANT(doc.IsObject());

    INVARIANT(doc.HasMember("syncFromHost"));
    INVARIANT(doc["syncFromHost"].IsString());
    result->syncFromHost = doc["syncFromHost"].GetString();

    INVARIANT(doc.HasMember("syncFromPort"));
    INVARIANT(doc["syncFromPort"].IsUint64());
    result->syncFromPort = static_cast<uint16_t>(
        doc["syncFromPort"].GetUint64());

    INVARIANT(doc.HasMember("id"));
    INVARIANT(doc["id"].IsUint64());
    result->id = (uint32_t)doc["id"].GetUint64();

    INVARIANT(doc.HasMember("syncFromId"));
    INVARIANT(doc["syncFromId"].IsUint64());
    result->syncFromId = static_cast<uint32_t>(doc["syncFromId"].GetUint64());

    INVARIANT(doc.HasMember("binlogId"));
    INVARIANT(doc["binlogId"].IsUint64());
    result->binlogId = static_cast<uint64_t>(doc["binlogId"].GetUint64());

    INVARIANT(doc.HasMember("replState"));
    INVARIANT(doc["replState"].IsUint64());
    result->replState = static_cast<ReplState>(doc["replState"].GetUint64());

#ifdef _WIN32
    return std::move(result);
#else
    return result;
#endif
}

Status Catalog::stop() {
    return _store->stop();
}

Status Catalog::setStoreMainMeta(const StoreMainMeta& meta) {
    std::stringstream ss;
    ss << "store_main_" << meta.id;
    RecordKey rk(0, 0, RecordType::RT_META, ss.str(), "");
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    writer.StartObject();

    writer.Key("Version");
    writer.String("1");

    writer.Key("storeMode");
    writer.Uint64(static_cast<uint8_t>(meta.storeMode));

    writer.Key("id");
    writer.Uint64(meta.id);

    writer.EndObject();

    RecordValue rv(sb.GetString(), RecordType::RT_META, -1);

    auto exptxn = _store->createTransaction(nullptr);
    if (!exptxn.ok()) {
        return exptxn.status();
    }

    Transaction *txn = exptxn.value().get();

    Record rd(std::move(rk), std::move(rv));
    Status s = _store->setKV(rd, txn);
    if (!s.ok()) {
        return s;
    }
    return txn->commit().status();
}

Expected<std::unique_ptr<StoreMainMeta>> Catalog::getStoreMainMeta(
                    uint32_t idx) {
    auto result = std::make_unique<StoreMainMeta>();
    std::stringstream ss;
    ss << "store_main_" << idx;
    RecordKey rk(0, 0, RecordType::RT_META, ss.str(), "");

    auto exptxn = _store->createTransaction(nullptr);
    if (!exptxn.ok()) {
        return exptxn.status();
    }

    Transaction *txn = exptxn.value().get();
    Expected<RecordValue> exprv = _store->getKV(rk, txn);
    if (!exprv.ok()) {
        return exprv.status();
    }
    RecordValue rv = exprv.value();
    const std::string& json = rv.getValue();

    rapidjson::Document doc;
    doc.Parse(json);
    if (doc.HasParseError()) {
        LOG(FATAL) << "parse meta failed"
            << rapidjson::GetParseError_En(doc.GetParseError());
    }

    INVARIANT(doc.IsObject());

    INVARIANT(doc.HasMember("id"));
    INVARIANT(doc["id"].IsUint64());
    result->id = (uint32_t)doc["id"].GetUint64();

    INVARIANT(doc.HasMember("storeMode"));
    INVARIANT(doc["storeMode"].IsUint64());
    result->storeMode = static_cast<KVStore::StoreMode>(
                        doc["storeMode"].GetUint64());

#ifdef _WIN32
    return std::move(result);
#else
    return result;
#endif
}

Status Catalog::setMainMeta(const MainMeta& meta) {
    std::stringstream ss;
    ss << "main_meta";
    RecordKey rk(0, 0, RecordType::RT_META, ss.str(), "");
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    writer.StartObject();

    writer.Key("Version");
    writer.String("1");

    writer.Key("kvStoreCount");
    writer.Uint64(static_cast<uint32_t>(meta.kvStoreCount));

    writer.Key("chunkSize");
    writer.Uint64(meta.chunkSize);

    writer.EndObject();

    RecordValue rv(sb.GetString(), RecordType::RT_META, -1);

    auto exptxn = _store->createTransaction(nullptr);
    if (!exptxn.ok()) {
        LOG(ERROR) << "Catalog::setMainMeta failed:" << exptxn.status().toString() << " " << sb.GetString();
        return exptxn.status();
    }

    Transaction *txn = exptxn.value().get();

    Record rd(std::move(rk), std::move(rv));
    Status s = _store->setKV(rd, txn);
    if (!s.ok()) {
        LOG(ERROR) << "Catalog::setMainMeta failed:" << s.toString() << " " << sb.GetString();
        return s;
    }
    LOG(INFO) << "Catalog::setMainMeta sucess:" << sb.GetString();
    return txn->commit().status();
}

Expected<std::unique_ptr<MainMeta>> Catalog::getMainMeta() {
    auto result = std::make_unique<MainMeta>();
    std::stringstream ss;
    ss << "main_meta";
    RecordKey rk(0, 0, RecordType::RT_META, ss.str(), "");

    auto exptxn = _store->createTransaction(nullptr);
    if (!exptxn.ok()) {
        return exptxn.status();
    }

    Transaction *txn = exptxn.value().get();
    Expected<RecordValue> exprv = _store->getKV(rk, txn);
    if (!exprv.ok()) {
        return exprv.status();
    }
    RecordValue rv = exprv.value();
    const std::string& json = rv.getValue();

    LOG(INFO) << "Catalog::getMainMeta succ," << json;

    rapidjson::Document doc;
    doc.Parse(json);
    if (doc.HasParseError()) {
        LOG(FATAL) << "parse meta failed"
            << rapidjson::GetParseError_En(doc.GetParseError());
    }

    INVARIANT(doc.IsObject());

    INVARIANT(doc.HasMember("kvStoreCount"));
    INVARIANT(doc["kvStoreCount"].IsUint64());
    result->kvStoreCount = (uint32_t)doc["kvStoreCount"].GetUint64();

    INVARIANT(doc.HasMember("chunkSize"));
    INVARIANT(doc["chunkSize"].IsUint64());
    result->chunkSize = (uint32_t)doc["chunkSize"].GetUint64();

#ifdef _WIN32
    return std::move(result);
#else
    return result;
#endif
}

}  // namespace tendisplus
