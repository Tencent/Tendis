// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <memory>
#include <thread>
#include <utility>

#include "tendisplus/commands/command.h"
#include "tendisplus/commands/release.h"
#include "tendisplus/commands/version.h"
#include "tendisplus/network/network.h"
#include "tendisplus/server/index_manager.h"
#include "tendisplus/server/segment_manager.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/sync_point.h"

// Number of key/values to place in database
static int FLAGS_num = 100000;

// Number of read operations to do.  If negative, do FLAGS_num reads.
// static int FLAGS_reads = -1;

// Number of concurrent threads to run.
static int FLAGS_threads = 10;

static bool FLAGS_directWriteRocksdb = false;
static bool FLAGS_ignoreKeyLock = false;

// Size of each value
// static int FLAGS_value_size = 100;

// Use the db with the following name.
static const char* FLAGS_db = nullptr;

static int FLAGS_port = 6379;
static int FLAGS_kvStoreCount = 10;

static bool FLAGS_binlogEnabled = false;
static bool FLAGS_binlogSaveLogs = false;

// Common key prefix length.
static int FLAGS_key_prefix = 0;

static bool FLAGS_generallog = false;
static bool FLAGS_checkkeytypeforsetcmd = false;
static int FLAGS_sleepAfterBenchmark = 0;

static int FLAGS_rocksTransactionMode = 1;

namespace tendisplus {
rocksdb::Env* g_env = nullptr;
std::shared_ptr<ServerEntry> g_server = nullptr;
using rocksdb::Slice;

static void AppendWithSpace(std::string* str, Slice msg);

class Stats {
 private:
  double start_;
  double finish_;
  double seconds_;
  int done_;
  int next_report_;
  int64_t bytes_;
  double last_op_finish_;
  std::string message_;

 public:
  Stats() {
    Start();
  }

  void Start() {
    next_report_ = 100;
    done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    message_.clear();
    start_ = finish_ = last_op_finish_ = g_env->NowMicros();
  }

  void Merge(const Stats& other) {
    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_)
      start_ = other.start_;
    if (other.finish_ > finish_)
      finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty())
      message_ = other.message_;
  }

  void Stop() {
    finish_ = g_env->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(Slice msg) {
    AppendWithSpace(&message_, msg);
  }

  void FinishedSingleOp() {
    done_++;
    if (done_ >= next_report_) {
      if (next_report_ < 1000)
        next_report_ += 100;
      else if (next_report_ < 5000)
        next_report_ += 500;
      else if (next_report_ < 10000)
        next_report_ += 1000;
      else if (next_report_ < 50000)
        next_report_ += 5000;
      else if (next_report_ < 100000)
        next_report_ += 10000;
      else if (next_report_ < 500000)
        next_report_ += 50000;
      else
        next_report_ += 100000;
      std::fprintf(stderr, "... finished %d ops%30s\r", done_, "");
      std::fflush(stderr);
    }
  }

  void AddBytes(int64_t n) {
    bytes_ += n;
  }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1)
      done_ = 1;

    std::string extra;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      double elapsed = (finish_ - start_) * 1e-6;
      char rate[100];
      std::snprintf(
        rate, sizeof(rate), "%6.1f MB/s", (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    AppendWithSpace(&extra, message_);

    std::fprintf(stdout,
                 "%-12s : %11.3f micros/op;%s%s\n",
                 name.ToString().c_str(),
                 seconds_ * 1e6 / done_,
                 (extra.empty() ? "" : " "),
                 extra.c_str());
    std::fflush(stdout);
  }
};

// Thinly wraps std::mutex.
class Mutex {
 public:
  Mutex() = default;
  ~Mutex() = default;

  Mutex(const Mutex&) = delete;
  Mutex& operator=(const Mutex&) = delete;

  void Lock() {
    mu_.lock();
  }
  void Unlock() {
    mu_.unlock();
  }
  void AssertHeld() {}

 private:
  friend class CondVar;
  std::mutex mu_;
};

// Thinly wraps std::condition_variable.
class CondVar {
 public:
  explicit CondVar(Mutex* mu) : mu_(mu) {
    assert(mu != nullptr);
  }
  ~CondVar() = default;

  CondVar(const CondVar&) = delete;
  CondVar& operator=(const CondVar&) = delete;

  void Wait() {
    std::unique_lock<std::mutex> lock(mu_->mu_, std::adopt_lock);
    cv_.wait(lock);
    lock.release();
  }
  void Signal() {
    cv_.notify_one();
  }
  void SignalAll() {
    cv_.notify_all();
  }

 private:
  std::condition_variable cv_;
  Mutex* const mu_;
};

class MutexLock {
 public:
  explicit MutexLock(Mutex* mu) : mu_(mu) {
    this->mu_->Lock();
  }
  ~MutexLock() {
    this->mu_->Unlock();
  }

  MutexLock(const MutexLock&) = delete;
  MutexLock& operator=(const MutexLock&) = delete;

 private:
  Mutex* const mu_;
};

// A very simple random number generator.  Not especially good at
// generating truly random bits, but good enough for our needs in this
// package.
class Random {
 private:
  uint32_t seed_;

 public:
  explicit Random(uint32_t s) : seed_(s & 0x7fffffffu) {
    // Avoid bad seeds.
    if (seed_ == 0 || seed_ == 2147483647L) {
      seed_ = 1;
    }
  }
  uint32_t Next() {
    static const uint32_t M = 2147483647L;  // 2^31-1
    static const uint64_t A = 16807;        // bits 14, 8, 7, 5, 2, 1, 0
    // We are computing
    //       seed_ = (seed_ * A) % M,    where M = 2^31-1
    //
    // seed_ must not be zero or M, or else all subsequent computed values
    // will be zero or M respectively.  For all other values, seed_ will end
    // up cycling through every number in [1,M-1]
    uint64_t product = seed_ * A;

    // Compute (product % M) using the fact that ((x << 31) % M) == x.
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    // The first reduction may overflow by 1 bit, so we may need to
    // repeat.  mod == M is not possible; using > allows the faster
    // sign-bit-based test.
    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }
  // Returns a uniformly distributed value in the range [0..n-1]
  // REQUIRES: n > 0
  uint32_t Uniform(int n) {
    return Next() % n;
  }

  // Randomly returns true ~"1/n" of the time, and false otherwise.
  // REQUIRES: n > 0
  bool OneIn(int n) {
    return (Next() % n) == 0;
  }

  // Skewed: pick "base" uniformly from range [0,max_log] and then
  // return "base" random bits.  The effect is to pick a number in the
  // range [0,2^max_log-1] with exponential bias towards smaller numbers.
  uint32_t Skewed(int max_log) {
    return Uniform(1 << Uniform(max_log + 1));
  }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  Mutex mu;
  CondVar cv;
  int total;

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  int num_initialized;
  int num_done;
  bool start;

  explicit SharedState(int total)
    : cv(&mu), total(total), num_initialized(0), num_done(0), start(false) {}
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;      // 0..n-1 when running in n threads
  Random rand;  // Has different seeds for different threads
  Stats stats;
  SharedState* shared;

  ThreadState(int index, int seed) : tid(index), rand(seed), shared(nullptr) {}
};

struct ThreadArg {
  SharedState* shared;
  ThreadState* thread;
  void (*method)(ThreadState*);
};

static void AppendWithSpace(std::string* str, Slice msg) {
  if (msg.empty())
    return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

bool setupEnv(const std::string& v) {
  std::error_code ec;
  std::stringstream ss;
  ss << "./" << v << "/log";
  filesystem::remove_all(ss.str(), ec);
  EXPECT_TRUE(ec.value() == 0 || ec.value() == 2);
  EXPECT_TRUE(filesystem::create_directories(ss.str()));

  ss.str("");
  ss << "./" << v << "/db";
  filesystem::remove_all(ss.str(), ec);
  EXPECT_TRUE(ec.value() == 0 || ec.value() == 2);
  EXPECT_TRUE(filesystem::create_directories(ss.str()));

  ss.str("");
  ss << "./" << v << "/dump";
  filesystem::remove_all(ss.str(), ec);
  EXPECT_TRUE(ec.value() == 0 || ec.value() == 2);
  EXPECT_TRUE(filesystem::create_directories(ss.str()));

  return true;
}

std::shared_ptr<ServerParams> GenServerParams() {
  std::stringstream ss_tmp_conf_file;
  ss_tmp_conf_file << getpid() << "_";
  ss_tmp_conf_file << FLAGS_port << "_test.cfg";
  std::string tmp_conf_file = ss_tmp_conf_file.str();

  const auto guard =
    MakeGuard([tmp_conf_file] { remove(tmp_conf_file.c_str()); });

  std::ofstream myfile;
  myfile.open(tmp_conf_file);
  myfile << "bind 127.0.0.1\n";
  myfile << "port " << FLAGS_port << "\n";
  myfile << "loglevel debug\n";
  if (FLAGS_db != nullptr) {
    myfile << "logdir ./" << FLAGS_db << "/log\n";
    myfile << "dir ./" << FLAGS_db << "/db\n";
    myfile << "dumpdir ./" << FLAGS_db << "/dump\n";
    myfile << "pidfile ./" << FLAGS_db << "/tendisplus.pid\n";
  } else {
    myfile << "logdir ./log\n";
    myfile << "dir ./db\n";
    myfile << "dumpdir ./dump\n";
    myfile << "pidfile ./tendisplus.pid\n";
  }
  myfile << "storage rocks\n";
  myfile << "rocks.blockcachemb 4096\n";
  myfile << "rocks.write_buffer_size 67108864\n";
  myfile << "rocks.compress_type lz4\n";
  myfile << "rocks.max_background_compactions 8\n";
  myfile << "rocks-transaction-mode " << FLAGS_rocksTransactionMode << "\n";

  myfile << "generallog " << FLAGS_generallog << "\n";
  myfile << "checkkeytypeforsetcmd " << FLAGS_checkkeytypeforsetcmd << "\n";

  myfile << "kvStoreCount " << FLAGS_kvStoreCount << "\n";
  myfile << "maxBinlogKeepNum 1000000\n";
  myfile << "slaveBinlogKeepNum 1000000\n";
  myfile << "ignorekeylock " << FLAGS_ignoreKeyLock << "\n";

#ifdef _WIN32
  myfile << "rocks.compress_type none\n";
#endif
  myfile.close();

  auto cfg = std::make_shared<ServerParams>();
  auto s = cfg->parseFile(tmp_conf_file);
  LOG(INFO) << "params:" << endl << cfg->showAll();
  if (!s.ok()) {
    LOG(FATAL) << "conf parseFile ret:" << s.toString();
  }
  EXPECT_EQ(s.ok(), true);

  return cfg;
}

void Open() {
  EXPECT_TRUE(setupEnv(FLAGS_db));

  auto cfg = GenServerParams();
  cfg->minBinlogKeepSec = 100;
  cfg->binlogEnabled = FLAGS_binlogEnabled;
  cfg->binlogSaveLogs = FLAGS_binlogSaveLogs;

  g_server = std::make_shared<ServerEntry>(cfg);
  auto s = g_server->startup(cfg);
  INVARIANT(s.ok());
}

class KeyBuffer {
 public:
  KeyBuffer() {
    assert(std::size_t(FLAGS_key_prefix) < sizeof(buffer_));
    memset(buffer_, 'a', FLAGS_key_prefix);
  }
  KeyBuffer& operator=(KeyBuffer& other) = delete;
  KeyBuffer(KeyBuffer& other) = delete;

  void Set(int k) {
    std::snprintf(buffer_ + FLAGS_key_prefix,
                  sizeof(buffer_) - FLAGS_key_prefix,
                  "%016d",
                  k);
  }

  Slice slice() const {
    return Slice(buffer_, FLAGS_key_prefix + 16);
  }

 private:
  char buffer_[1024];
};

void DoWriteKV(ThreadState* thread) {
  std::fprintf(stdout, "Thread %d write (%d ops) \n", thread->tid, FLAGS_num);
  auto ctx = std::make_shared<asio::io_context>();
  asio::ip::tcp::socket socket(*ctx);
  auto sess = std::make_shared<NetSession>(
    g_server, std::move(socket), 0, false, nullptr, nullptr);

  for (int i = 0; i < FLAGS_num; ++i) {
    KeyBuffer key;
    key.Set(thread->rand.Uniform(FLAGS_num));
    auto keyStr = std::to_string(thread->tid) + key.slice().ToString();
    sess->setArgs({"set", keyStr, std::to_string(i)});
    auto expect = Command::runSessionCmd(sess.get());
    thread->stats.FinishedSingleOp();
    EXPECT_TRUE(expect.status().ok());
    if (expect.status().ok()) {
      EXPECT_EQ(expect.value(), Command::fmtOK());
    }
  }
}

void DoWriteKVToRocksdb(ThreadState* thread) {
  std::fprintf(stdout, "Thread %d write (%d ops) \n", thread->tid, FLAGS_num);
  auto ctx = std::make_shared<asio::io_context>();
  asio::ip::tcp::socket socket(*ctx);
  auto sess = std::make_shared<NetSession>(
    g_server, std::move(socket), 0, false, nullptr, nullptr);

  for (int i = 0; i < FLAGS_num; ++i) {
    KeyBuffer key;
    key.Set(thread->rand.Uniform(FLAGS_num));
    auto keyStr = std::to_string(thread->tid) + key.slice().ToString();
    auto value = std::to_string(i);
    sess->setArgs({"set", keyStr, std::to_string(i)});

    auto expdb = g_server->getSegmentMgr()->getDbWithKeyLock(
      sess.get(), keyStr, mgl::LockMode::LOCK_NONE);
    EXPECT_TRUE(expdb.ok());

    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    EXPECT_TRUE(ptxn.ok());

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    RecordKey rk(
      expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_KV, keyStr, "");

    uint64_t ts = 0;
    RecordValue rv(value, RecordType::RT_KV, pCtx->getVersionEP(), ts);

    Status status = kvstore->setKV(rk, rv, ptxn.value());
    TEST_SYNC_POINT("setGeneric::SetKV::1");
    EXPECT_TRUE(status.ok());
    Expected<uint64_t> exptCommit =
      sess->getCtx()->commitTransaction(ptxn.value());
    EXPECT_TRUE(exptCommit.ok());

    thread->stats.FinishedSingleOp();
  }
}

static void ThreadBody(void* v) {
  ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
  SharedState* shared = arg->shared;
  ThreadState* thread = arg->thread;
  {
    MutexLock l(&shared->mu);
    shared->num_initialized++;
    if (shared->num_initialized >= shared->total) {
      shared->cv.SignalAll();
    }
    while (!shared->start) {
      shared->cv.Wait();
    }
  }

  thread->stats.Start();
  (*(arg->method))(thread);
  thread->stats.Stop();

  {
    MutexLock l(&shared->mu);
    shared->num_done++;
    if (shared->num_done >= shared->total) {
      shared->cv.SignalAll();
    }
  }
}

void RunBenchmark(int n, Slice name, void (*method)(ThreadState*)) {
  SharedState shared(n);
  int total_thread_count_ = 0;

  ThreadArg* arg = new ThreadArg[n];
  for (int i = 0; i < n; i++) {
    arg[i].method = method;
    arg[i].shared = &shared;
    ++total_thread_count_;
    // Seed the thread's random state deterministically based upon thread
    // creation across all benchmarks. This ensures that the seeds are unique
    // but reproducible when rerunning the same set of benchmarks.
    arg[i].thread = new ThreadState(i, /*seed=*/1000 + total_thread_count_);
    arg[i].thread->shared = &shared;
    g_env->StartThread(ThreadBody, &arg[i]);
  }

  shared.mu.Lock();
  while (shared.num_initialized < n) {
    shared.cv.Wait();
  }

  shared.start = true;
  shared.cv.SignalAll();
  while (shared.num_done < n) {
    shared.cv.Wait();
  }
  shared.mu.Unlock();

  for (int i = 1; i < n; i++) {
    arg[0].thread->stats.Merge(arg[i].thread->stats);
  }
  arg[0].thread->stats.Report(name);

  for (int i = 0; i < n; i++) {
    delete arg[i].thread;
  }
  delete[] arg;
}

void Run() {
  Open();
  auto method = &DoWriteKV;
  if (FLAGS_directWriteRocksdb) {
    method = &DoWriteKVToRocksdb;
  }
  rocksdb::Slice name("kvwrite");
  LOG(INFO) << "Start benchmark :" << name.data();
  auto start = g_env->NowMicros();
  RunBenchmark(FLAGS_threads, name, method);
  auto end = g_env->NowMicros();
  LOG(INFO) << "End benchmark :" << name.data()
            << "QPS:" << (FLAGS_num * FLAGS_threads * 1e6) / (end - start);
  g_env->SleepForMicroseconds(FLAGS_sleepAfterBenchmark * 1000000);
#ifndef _WIN32
  g_server->stop();
#endif
  g_server = nullptr;
}

}  // namespace tendisplus

void printVersionInfo() {
  // db_stress tool, Tendisplus v=2.4.3-rocksdb-v6.23.3
  //   sha=f4600de9 dirty=23 build=tlinux-1650438040
  std::cout << "db_stress tool, Tendisplus v=" << getTendisPlusVersion()
            << " sha=" << TENDISPLUS_GIT_SHA1
            << " dirty=" << TENDISPLUS_GIT_DIRTY
            << " build=" << TENDISPLUS_BUILD_ID << std::endl;
}

void printHelpInfo() {
  /*
  db_stress -- Benchmark Tool for Tendisplus
  usage: db_stress [options]
    Options:
      --binlogEnabled=n             enable binlog for benchmark. 0 = off / 1 =
  on
      --binlogSaveLogs=n            save binlog for benchmark. 0 = off / 1 = on
      --db=path                     set path used in benchmark.
      --kvStoreCount=n              set kvstorecount used in benchmark.
      --generallog=n                enable log general for benchmark. 0 = off /
  1 = on
      --sleepAfterBenchmark=seconds sleepping time after test.
      --num=n                       kvwrite options number
      --thread=n                    work thread number
      --rocksTransactionMode=mode   txn mode for Tendisplus.
                                      0 for Optimistic Transaction.
                                      1 for Pessimistic Transaction.
                                      2 for WriteBatch
      -h/--help                     print help info
      -v/--version                  print version info
  */
  std::cout << "db_stress -- Benchmark Tool for Tendisplus" << std::endl;
  std::cout << "usage: db_stress [options]" << std::endl;
  std::cout << "  Options:" << std::endl;
  std::cout << "    --binlogEnabled=n             "
            << "enable binlog for benchmark. 0 = off / 1 = on" << std::endl;
  std::cout << "    --binlogSaveLogs=n            "
            << "save binlog for benchmark. 0 = off / 1 = on" << std::endl;
  std::cout << "    --db=path                     "
            << "set path used in benchmark." << std::endl;
  std::cout << "    --kvStoreCount=n              "
            << "set kvstorecount used in benchmark." << std::endl;
  std::cout << "    --generallog=n                "
            << "enable log general for benchmark. "
            << "0 = off / 1 = on" << std::endl;
  std::cout << "    --sleepAfterBenchmark=seconds "
            << "sleepping time after test." << std::endl;
  std::cout << "    --num=n                       "
            << "kvwrite options number" << std::endl;
  std::cout << "    --thread=n                    "
            << "work thread number" << std::endl;
  std::cout << "    --rocksTransactionMode=mode   "
            << "txn mode for Tendisplus." << std::endl;
  std::cout << "                                  "
            << "  0 for Optimistic Transaction." << std::endl;
  std::cout << "                                  "
            << "  1 for Pessimistic Transaction." << std::endl;
  std::cout << "                                  "
            << "  2 for WriteBatch" << std::endl;
  std::cout << "    -h/--help                     "
            << "print help info" << std::endl;
  std::cout << "    -v/--version                  "
            << "print version info" << std::endl;
}

int main(int argc, char** argv) {
  std::string default_db_path;

  for (int i = 1; i < argc; i++) {
    int n;
    char junk;
    if (sscanf(argv[i], "--directWriteRocksdb=%d%c", &n, &junk) &&
        (n == 0 || n == 1)) {
      FLAGS_directWriteRocksdb = n;
    } else if (sscanf(argv[i], "--ignoreKeyLock=%d%c", &n, &junk) &&
               (n == 0 || n == 1)) {
      FLAGS_ignoreKeyLock = n;
    } else if (sscanf(argv[i], "--binlogEnabled=%d%c", &n, &junk) &&
               (n == 0 || n == 1)) {
      FLAGS_binlogEnabled = n;
    } else if (sscanf(argv[i], "--binlogSaveLogs=%d%c", &n, &junk) &&
               (n == 0 || n == 1)) {
      FLAGS_binlogSaveLogs = n;
    } else if (strncmp(argv[i], "--db=", strlen("--db=")) == 0) {
      FLAGS_db = argv[i] + 5;
    } else if (sscanf(argv[i], "--kvStoreCount=%d%c", &n, &junk) == 1) {
      FLAGS_kvStoreCount = n;
    } else if (sscanf(argv[i], "--generallog=%d%c", &n, &junk) &&
               (n == 0 || n == 1)) {
      FLAGS_generallog = n;
    } else if (sscanf(argv[i], "--sleepAfterBenchmark=%d%c", &n, &junk) == 1) {
      FLAGS_sleepAfterBenchmark = n;
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--thread=%d%c", &n, &junk) == 1) {
      FLAGS_threads = n;
    } else if (strncmp(argv[i], "-v", strlen("-v")) == 0) {
      printVersionInfo();
      std::exit(0);
    } else if (strncmp(argv[i], "--version", strlen("--version")) == 0) {
      printVersionInfo();
      std::exit(0);
    } else if (strncmp(argv[i], "-h", strlen("-h")) == 0) {
      printHelpInfo();
      std::exit(0);
    } else if (strncmp(argv[i], "--help", strlen("--help")) == 0) {
      printHelpInfo();
      std::exit(0);
    } else if (sscanf(argv[i], "--rocksTransactionMode=%d%c", &n, &junk) == 1) {
      FLAGS_rocksTransactionMode = n;
    } else {
      std::fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      printHelpInfo();
      std::exit(1);
    }
  }

  tendisplus::g_env = rocksdb::Env::Default();

  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db == nullptr) {
    tendisplus::g_env->GetTestDirectory(&default_db_path);
    default_db_path += "/dbbench";
    FLAGS_db = default_db_path.c_str();
  }

  // const auto guard = MakeGuard([] {
  //  destroyEnv(single_dir);
  //  std::this_thread::sleep_for(std::chrono::seconds(5));
  //});

  tendisplus::Run();

  return 0;
}
