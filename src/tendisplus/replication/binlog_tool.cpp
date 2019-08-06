#include <iostream>
#include <string>
#include <vector>
#include "tendisplus/utils/param_manager.h"
#include "tendisplus/utils/base64.h"
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/storage/varint.h"
#include "tendisplus/storage/record.h"

namespace tendisplus {

using namespace std;

// TODO(takenliu) print error to stderr or logfile?
class BinlogScanner{
 public:
    enum TOOL_MODE {
        TEXT_SHOW = 0,
        BASE64_SHOW,
    };

    void init(const tendisplus::ParamManager& pm) {
        _logfile = pm.getString("logfile");
        _startDatetime = pm.getUint64("start-datetime", _startDatetime);
        _endDatetime = pm.getUint64("end-datetime", _endDatetime);
        _startPosition = pm.getUint64("start-position", _startPosition);
        _endPosition = pm.getUint64("end-position", _endPosition);
        _mode = TOOL_MODE::TEXT_SHOW;
        if (pm.getString("mode") == "base64") {
            _mode = TOOL_MODE::BASE64_SHOW;
        }
    }

    bool isFiltered(const ReplLogKeyV2& logkey,
        const ReplLogValueV2& logValue) {
        if (logkey.getBinlogId() < _startPosition
            || logkey.getBinlogId() > _endPosition) {
            return true;
        }
        if (logValue.getTimestamp() < _startDatetime
            || logValue.getTimestamp() > _endDatetime) {
            return true;
        }
        return false;
    }

    void process(const string& key, const string& value, uint32_t storeId) {
        Expected<ReplLogKeyV2> logkey = ReplLogKeyV2::decode(key);
        if (!logkey.ok()) {
            cerr << "decode logkey failed." << endl;
            return;
        }

        Expected<ReplLogValueV2> logValue = ReplLogValueV2::decode(value);
        if (!logValue.ok()) {
            cerr << "decode logvalue failed." << endl;
            return;
        }

        if (isFiltered(logkey.value(), logValue.value())) {
            return;
        }

        if (_mode == TOOL_MODE::TEXT_SHOW) {
            cout << "storeid:" << storeId
                << " binlogid:" << logkey.value().getBinlogId()
                << " txnid:" << logValue.value().getTxnId()
                << " chunkid:" << logValue.value().getChunkId()
                << " ts:" << logValue.value().getTimestamp() << endl;
            Expected<std::vector<ReplLogValueEntryV2>> logList =
                logValue.value().getLogList();
            if (!logList.ok()) {
                cerr << "decode logList failed." << endl;
                return;
            }
            for (auto oneLog : logList.value()) {
                Expected<RecordKey> opkey =
                    RecordKey::decode(oneLog.getOpKey());
                Expected<RecordValue> opvalue =
                    RecordValue::decode(oneLog.getOpValue());
                if (!opkey.ok() || !opvalue.ok()) {
                    cerr << "decode opkey or opvalue failed.";
                    return;
                }
                cout << "  op:" << (uint32_t)oneLog.getOp()
                    << " fkey:" << opkey.value().getPrimaryKey()
                    << " skey:" << opkey.value().getSecondaryKey()
                    << " opvalue:" << opvalue.value().getValue() << endl;
            }
        } else if (_mode == TOOL_MODE::BASE64_SHOW) {
            string baseKey =
                Base64::Encode((unsigned char*)key.c_str(), key.size());
            string baseValue =
                Base64::Encode((unsigned char*)value.c_str(), value.size());
            std::cout << "restorebinlogv2 " << storeId
                << " " << baseKey << " " << baseValue << endl;
        }
    }

    void scan() {
        FILE* pf = fopen(_logfile.c_str(), "r");
        if (pf == NULL) {
            cerr << "fopen failed:" << _logfile << endl;
            return;
        }
        const uint32_t buff_len = 4096;
        char* buff = new char[buff_len];

        int ret = fread(buff, BINLOG_HEADER_V2_LEN, 1, pf);
        if (ret != 1 || strstr(buff, BINLOG_HEADER_V2) != buff) {
            cerr << "read head failed." << endl;
            return;
        }
        buff[BINLOG_HEADER_V2_LEN] = '\0';
        uint32_t storeId =
            be32toh(*reinterpret_cast<uint32_t*>(buff + strlen(BINLOG_HEADER_V2)));

        while (!feof(pf)) {
            // keylen
            uint32_t keylen = 0;
            ret = fread(buff, sizeof(uint32_t), 1, pf);
            if (ret != 1) {
                if (feof(pf)) {
                    cerr << "read logfile end." << endl;
                    return; // read file end.
                }
                cerr << "read keylen failed." << endl;
                return;
            }
            buff[sizeof(uint32_t)] = '\0';
            keylen = int32Decode(buff);

            // key
            string key;
            key.resize(keylen);
            ret = fread(const_cast<char*>(key.c_str()), keylen, 1, pf);
            if (ret != 1) {
                cerr << "read key failed." << endl;
                return;
            }

            // valuelen
            uint32_t valuelen = 0;
            ret = fread(buff, sizeof(uint32_t), 1, pf);
            if (ret != 1) {
                cerr << "read valuelen failed." << endl;
                return;
            }
            buff[sizeof(uint32_t)] = '\0';
            valuelen = int32Decode(buff);

            // value
            string value;
            value.resize(valuelen);
            ret = fread(const_cast<char*>(value.c_str()),
                valuelen, 1, pf);
            if (ret != 1) {
                cerr << "read value failed." << endl;
                return;
            }

            process(key, value, storeId);
        }
        delete[] buff;
        fclose(pf);
    }

 private:
    string _logfile;
    TOOL_MODE _mode;
    uint64_t _startDatetime = 0;
    uint64_t _endDatetime = UINT64_MAX;
    uint64_t _startPosition = 0;
    uint64_t _endPosition = UINT64_MAX;
};

}  // namespace tendisplus

void usage() {
    std::cerr << "binlog_tool --logfile=binlog.log --mode=text|base64"
        << " --start-datetime=1111 --end-datetime=22222"
        << " --start-position=333333 --end-position=55555"
        << /*" --keys=1,2,4,5,6,7,8,9" <<*/ std::endl;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        usage();
        return 0;
    }
    tendisplus::ParamManager pm;
    pm.init(argc, argv);

    tendisplus::BinlogScanner bs;
    bs.init(pm);
    bs.scan();
    return 0;
}

