#include <sstream>
#include <utility>
#include <memory>
#include "glog/logging.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h"
#include "tendisplus/storage/catalog.h"

namespace tendisplus {

Catalog::Catalog(std::unique_ptr<KVStore> store)
    :_store(std::move(store)) {
}

Status Catalog::setStoreMeta(const StoreMeta& meta) {
    std::stringstream ss;
    ss << "store_" << meta.id;
    RecordKey rk(0, RecordType::RT_META, ss.str(), "");
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    writer.StartObject();
    writer.Key("Version");
    writer.String("1");
    writer.Key("syncFromHost");
    writer.String(meta.syncFromHost);
    writer.Key("id");
    writer.Uint64(meta.id);
    RecordValue rv(sb.GetString());

    auto exptxn = _store->createTransaction();
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

Expected<StoreMeta> Catalog::getStoreMeta(uint32_t idx) {
    StoreMeta result;
    std::stringstream ss;
    ss << "store_" << idx;
    RecordKey rk(0, RecordType::RT_META, ss.str(), "");

    auto exptxn = _store->createTransaction();
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

    assert(doc.IsObject());
    assert(doc.HasMember("syncFromHost"));
    assert(doc["syncFromHost"].IsString());
    result.syncFromHost = doc["syncFromHost"].GetString();
    assert(doc.HasMember("id"));
    assert(doc["id"].IsUint64());
    result.id = doc["id"].GetUint64();

    return result;
}

}  // namespace tendisplus
