package main

import (
    "flag"
    "fmt"
    "github.com/mediocregopher/radix.v2/redis"
    "github.com/ngaut/log"
    "math"
    "tendisplus/integrate_test/util"
    "time"
    "os"
    "path"
    "path/filepath"
    "strings"
    "os/exec"
    "strconv"
    "sort"
)

var (
    m1port     = flag.Int("master1port", 61001, "master1 port")
    s1port     = flag.Int("slave1port", 61002, "slave1 port")
    s2port     = flag.Int("slave2port", 61003, "slave2 port")
    m2port     = flag.Int("master2port", 61004, "master2 port")

    m1ip = flag.String("master1ip", "127.0.0.1", "master1 ip")
    s1ip = flag.String("slave1ip", "127.0.0.1", "slave1 ip")
    s2ip = flag.String("slave2ip", "127.0.0.1", "slave2 ip")
    m2ip = flag.String("master2ip", "127.0.0.1", "master2 ip")

    num1     = flag.Int("num1", 100, "first add data nums")
    num2     = flag.Int("num2", 100, "first add data nums")
    shutdown = flag.Int("shutdown", 1, "whether shutdown the dir")
    clear    = flag.Int("clear", 1, "whether clear the dir")
    startup  = flag.Int("startup", 1, "whether startup")
    kvstorecount     = flag.Int("kvstorecount", 10, "kvstore count")
)

func getCurrentDirectory() string {
    dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
    if err != nil {
        log.Fatal(err)
    }
    return strings.Replace(dir, "\\", "/", -1)
}

func addDataInCoroutine(m *util.RedisServer, num int, prefixkey string, channel chan int) {
    addData(m, num, prefixkey)
    channel <- 0
}

func addData(m *util.RedisServer, num int, prefixkey string) {
    log.Infof("addData begin. %s:%d", m.Ip, m.Port)

    // "set,incr,lpush,lpop,sadd,spop,hset,mset"
    cmd := exec.Command("./redis-benchmark", "-h", m.Ip, "-p", strconv.Itoa(m.Port),
        "-c", "20", "-n", strconv.Itoa(num), "-r", "8", "-i", "-f", prefixkey, "-t", "set,incr,lpush,sadd,hset")
    _, err := cmd.Output()
    //fmt.Print(string(output))
    if err != nil {
        fmt.Print(err)
    }
    log.Infof("addData sucess. %s:%d num:%d", m.Ip, m.Port, num)
}

func addOnekeyEveryStore(m *util.RedisServer, kvstorecount int) {
    cli, err := redis.DialTimeout("tcp", fmt.Sprintf("%s:%d", m.Ip, m.Port), 10*time.Second)
    if err != nil {
        log.Fatalf("can't connect to %d: %v", m.Port, err)
    }
    for i := 0; i < kvstorecount; i++ {
        if r, err := cli.Cmd("setinstore", strconv.Itoa(i), "fixed_test_key", "fixed_test_value").Str(); err != nil {
            log.Fatalf("do addOnekeyEveryStore %d failed:%v", i, err)
        } else if r != "OK" {
            log.Fatalf("do addOnekeyEveryStore error:%s", r)
            return
        }
    }
    log.Infof("addOnekeyEveryStore sucess.port:%d", m.Port)
}

func backup(m *util.RedisServer) {
    cli, err := redis.DialTimeout("tcp", fmt.Sprintf("%s:%d", m.Ip, m.Port), 10*time.Second)
    if err != nil {
        log.Fatalf("can't connect to %d: %v", m.Port, err)
    }

    os.RemoveAll("/tmp/back_test")
    os.Mkdir("/tmp/back_test", os.ModePerm)
    if r, err := cli.Cmd("backup", "/tmp/back_test").Str(); err != nil {
        log.Fatalf("do backup failed:%v", err)
        return
    } else if r != "OK" {
        log.Fatalf("do backup error:%s", r)
        return
    }
    log.Infof("backup sucess,port:%d" , m.Port)
}

func slaveof(m *util.RedisServer, s *util.RedisServer) {
    cli, err := redis.DialTimeout("tcp", fmt.Sprintf("%s:%d", s.Ip, s.Port), 10*time.Second)
    if err != nil {
        log.Fatalf("can't connect to %d: %v", s.Port, err)
    }

    if r, err := cli.Cmd("slaveof", m.Ip, strconv.Itoa(m.Port)).Str(); err != nil {
        log.Fatalf("do slaveof failed:%v", err)
        return
    } else if r != "OK" {
        log.Fatalf("do slaveof error:%s", r)
        return
    }
    log.Infof("slaveof sucess,mport:%d sport:%d" , m.Port, s.Port)
}

func restoreBackup(m *util.RedisServer) {
    cli, err := redis.DialTimeout("tcp", fmt.Sprintf("%s:%d", m.Ip, m.Port), 10*time.Second)
    if err != nil {
        log.Fatalf("can't connect to %d: %v", m.Port, err)
    }

    if r, err := cli.Cmd("restorebackup", "all", "/tmp/back_test", "force").Str(); err != nil {
        log.Fatalf("do restorebackup failed:%v", err)
    } else if r != "OK" {
        log.Fatalf("do restorebackup error:%s", r)
    }
    log.Infof("restorebackup sucess,port:%d" , m.Port)
}

func waitFullsyncInCoroutine(s *util.RedisServer, kvstorecount int, channel chan int) {
    waitFullsync(s, kvstorecount)
    channel <- 0
}

func waitFullsync(s *util.RedisServer, kvstorecount int) {
    log.Infof("waitFullsync begin.sport:%d", s.Port)
    cli2, err2 := redis.DialTimeout("tcp", fmt.Sprintf("%s:%d", s.Ip, s.Port), 10*time.Second)
    if err2 != nil {
        log.Fatalf("can't connect to %d: %v", s.Port, err2)
    }
    for i := 0; i < kvstorecount; i++ {
        for {
            var replstatus int
            if r, err := cli2.Cmd("replstatus", i).Int(); err != nil {
                log.Fatalf("do waitFullsync %d failed:%v", i, err)
            }else {
                //log.Infof("binlogpos store:%d binlogmax:%d" , i, r)
                replstatus = r
            }

            if replstatus != 3{
                time.Sleep(100*1000000) // 100ms
            } else {
                break;
            }
        }
    }
    log.Infof("waitFullsync sucess.sport:%d", s.Port)
}

func waitCatchupInCoroutine(m *util.RedisServer, s *util.RedisServer, kvstorecount int, channel chan int) {
    waitCatchup(m, s, kvstorecount)
    channel <- 0
}

func waitCatchup(m *util.RedisServer, s *util.RedisServer, kvstorecount int) {
    log.Infof("waitCatchup begin.mport:%d sport:%d", m.Port, s.Port)
    cli1, err1 := redis.DialTimeout("tcp", fmt.Sprintf("%s:%d", m.Ip, m.Port), 10*time.Second)
    if err1 != nil {
        log.Fatalf("can't connect to %d: %v", m.Port, err1)
    }
    cli2, err2 := redis.DialTimeout("tcp", fmt.Sprintf("%s:%d", s.Ip, s.Port), 10*time.Second)
    if err2 != nil {
        log.Fatalf("can't connect to %d: %v", s.Port, err2)
    }
    var loop_time int = 0
    for {
        loop_time++
        var same bool = true
        var total1 int = 0
        var total2 int = 0
        for i := 0; i < kvstorecount; i++ {
            var binlogmax1 int
            if r, err := cli1.Cmd("binlogpos", i).Int(); err != nil {
                log.Fatalf("do waitCatchup %d failed:%v", i, err)
            }else {
                //log.Infof("binlogpos store:%d binlogmax:%d" , i, r)
                binlogmax1 = r
            }

            var binlogmax2 int
            if r, err := cli2.Cmd("binlogpos", i).Int(); err != nil {
                log.Fatalf("do waitCatchup %d failed:%v", i, err)
            }else {
                //log.Infof("binlogpos store:%d binlogmax:%d" , i, r)
                binlogmax2 = r
            }

            if binlogmax1 != binlogmax2 {
                same = false
            }
            if loop_time % 50 == 0 {
                diff := binlogmax1 - binlogmax2
                total1 += binlogmax1
                total2 += binlogmax2
                log.Infof("waitCatchup.mport:%d sport:%d storeid:%d binlogmax1:%d binlogmax2:%d diff:%d",
                    m.Port, s.Port, i, binlogmax1, binlogmax2, diff)
            }
        }
        if loop_time % 50 == 0 {
            log.Infof("waitCatchup.mport:%d sport:%d total1:%d total2:%d total_diff:%d",
                m.Port, s.Port, total1, total2, total1 - total2)
        }
        if same {
            break;
        } else {
            time.Sleep(100*1000000) // 100ms
        }
    }
    log.Infof("waitCatchup sucess.mport:%d sport:%d", m.Port, s.Port)
}

func waitDumpBinlog(m *util.RedisServer, kvstorecount int) {
    cli, err := redis.DialTimeout("tcp", fmt.Sprintf("%s:%d", m.Ip, m.Port), 10*time.Second)
    if err != nil {
        log.Fatalf("can't connect to %d: %v", m.Port, err)
    }
    for i := 0; i < kvstorecount; i++ {
        for {
            var binlogmin int
            var binlogmax int
            if r, err := cli.Cmd("binlogpos", i).Int(); err != nil {
                log.Fatalf("do waitDumpBinlog %d failed:%v", i, err)
            }else {
                //log.Infof("binlogpos store:%d binlogmax:%d" , i, r)
                binlogmax = r
            }
    
            if r, err := cli.Cmd("binlogstart", i).Int(); err != nil {
                log.Fatalf("do waitDumpBinlog %d failed:%v", i, err)
            }else {
                //log.Infof("binlogpos store:%d binlogmin:%d" , i, r)
                binlogmin = r
            }
            if binlogmin != binlogmax {
                time.Sleep(100*1000000) // 100ms
            } else {
                break;
            }
        }
    }
    log.Infof("waitDumpBinlog sucess.port:%d", m.Port)
}

func flushBinlog(m *util.RedisServer) {
    cli, err := redis.DialTimeout("tcp", fmt.Sprintf("%s:%d", m.Ip, m.Port), 10*time.Second)
    if err != nil {
        log.Fatalf("can't connect to %d: %v", m.Port, err)
    }

    if r, err := cli.Cmd("binlogflush", "all").Str(); err != nil {
        log.Fatalf("do flushBinlog failed:%v", err)
    } else if r != "OK" {
        log.Fatalf("do flushBinlog error:%s", r)
    }
    log.Infof("flushBinlog sucess,port:%d" , m.Port)
}

func pipeRun(commands []*exec.Cmd) {
    for i:= 1;i < len(commands);i++{
        commands[i].Stdin, _ = commands[i-1].StdoutPipe()
    }
    // commands[len(commands)-1].Stdout = os.Stdout
    
    for i:=1;i<len(commands);i++{
        err := commands[i].Start()
        if err != nil {
            panic(err)
        }
    }
    commands[0].Run()
    
    for i:=1;i<len(commands);i++{
        err := commands[i].Wait()
        if err != nil {
            panic(err)
        }
    }
}

func restoreBinlog(m1 *util.RedisServer, m2 *util.RedisServer, kvstorecount int) {
    var channel chan int = make(chan int)
    for i := 0; i < kvstorecount; i++ {
        go restoreBinlogInCoroutine(m1, m2, i, channel)
    }
    for i := 0; i < kvstorecount; i++ {
        <- channel
    }
    log.Infof("restoreBinlog sucess,port:%d" , m2.Port)
}

func restoreBinlogInCoroutine(m1 *util.RedisServer, m2 *util.RedisServer, storeId int, channel chan int) {
    cli, err := redis.DialTimeout("tcp", fmt.Sprintf("%s:%d", m2.Ip, m2.Port), 10*time.Second)
    if err != nil {
        log.Fatalf("can't connect to %d: %v", m2.Port, err)
    }

    var binlogPos int
    if r, err := cli.Cmd("binlogpos", storeId).Int(); err != nil {
        log.Fatalf("do restoreBinlog %d failed:%v", storeId, err)
    }else {
        //log.Infof("binlogpos store:%d binlogmax:%d" , storeId, r)
        binlogPos = r
    }
 
    subpath := m1.Path + "/dump/" + strconv.Itoa(storeId) + "/";
    files, _ := filepath.Glob(subpath + "binlog*.log")
    if len(files) <= 0 {
        return;
    }
    sort.Strings(files)

    var endTs uint64 = math.MaxUint64
    for j := 0; j < len(files); j++ {
        var commands []*exec.Cmd
        commands = append(commands, exec.Command("./binlog_tool",
            "--logfile=" + files[j],
            "--mode=base64",
            "--start-position=" + strconv.Itoa(binlogPos),
            "--end-datetime=" + strconv.FormatUint(endTs, 10),
            ))
        commands = append(commands, exec.Command("./redis-cli",
            "-p", strconv.Itoa(m2.Port)))
        pipeRun(commands)

        log.Infof("restoreBinlog sucess store:%d binlogPos:%d file:%s" , storeId, binlogPos, path.Base(files[j]))
    }
    log.Infof("restoreBinlog sucess,port:%d storeid:%d" , m2.Port, storeId)
    channel <- 0
}

func shutdownServer(m *util.RedisServer, shutdown int, clear int) {
    if (shutdown <= 0) {
        return;
    }
    cli, err := redis.DialTimeout("tcp", fmt.Sprintf("%s:%d", m.Ip, m.Port), 10*time.Second)
    if err != nil {
        log.Fatalf("can't connect to %d: %v", m.Port, err)
    }

    if r, err := cli.Cmd("shutdown").Str(); err != nil {
        log.Fatalf("do shutdown failed:%v", err)
    } else if r != "OK" {
        log.Fatalf("do shutdown error:%s", r)
    }
    if (clear > 0) {
        m.Destroy();
    }
    log.Infof("shutdownServer server,port:%d", m.Port)
}

func compareInCoroutine(m1 *util.RedisServer, m2 *util.RedisServer, channel chan int) {
    compare(m1, m2)
    channel <- 0
}

func compare(m1 *util.RedisServer, m2 *util.RedisServer) {
    cmd := exec.Command("./compare_instances", fmt.Sprintf("%s:%d", m1.Ip, m1.Port), fmt.Sprintf("%s:%d", m2.Ip, m2.Port))
    cmd.Stderr = os.Stderr
    output, err := cmd.Output()
    fmt.Print("Command output:\n", string(output))
    if err != nil {
        fmt.Println("Command err:", err)
        log.Infof("compare failed.")
        return
    }
    log.Infof("compare sucess,port1:%d port2:%d", m1.Port, m2.Port)
}

