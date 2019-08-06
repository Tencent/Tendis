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
    num1     = flag.Int("num1", 1000, "first add data nums")
    num2     = flag.Int("num2", 1000, "first add data nums")
    kvstorecount     = flag.Int("kvstorecount", 10, "kvstore count")
)

func getCurrentDirectory() string {
    dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
    if err != nil {
        log.Fatal(err)
    }
    return strings.Replace(dir, "\\", "/", -1)
}

func addData(port int, num int, prefixkey string) {
    log.Infof("addData begin.port:%d", port)

    // "set,incr,lpush,lpop,sadd,spop,hset,mset"
    cmd := exec.Command("/data/home/takenliu/git/redis-for-cloud/src/redis-benchmark", "-p", strconv.Itoa(port), "-c", "1", "-n", strconv.Itoa(num), "-r", "8", "-i", "-f", prefixkey, "-t", "set,incr,lpush,lpop,sadd,spop,hset")
    _, err := cmd.Output()
    //fmt.Print(string(output))
    if err != nil {
        fmt.Print(err)
    }
    log.Infof("addData sucess.port:%d", port)
}

func addOnekeyEveryStore(m *util.RedisServer, kvstorecount int) {
    cli, err := redis.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", m.Port), 10*time.Second)
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
    cli, err := redis.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", m.Port), 10*time.Second)
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
    cli, err := redis.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", s.Port), 10*time.Second)
    if err != nil {
        log.Fatalf("can't connect to %d: %v", s.Port, err)
    }

    if r, err := cli.Cmd("slaveof", "127.0.0.1", strconv.Itoa(m.Port)).Str(); err != nil {
        log.Fatalf("do slaveof failed:%v", err)
        return
    } else if r != "OK" {
        log.Fatalf("do slaveof error:%s", r)
        return
    }
    log.Infof("slaveof sucess,port:%d" , m.Port)
}

func restoreBackup(m *util.RedisServer) {
    cli, err := redis.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", m.Port), 10*time.Second)
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

func waitCatchup(m *util.RedisServer, s *util.RedisServer, kvstorecount int) {
    cli1, err1 := redis.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", m.Port), 10*time.Second)
    if err1 != nil {
        log.Fatalf("can't connect to %d: %v", m.Port, err1)
    }
    cli2, err2 := redis.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", s.Port), 10*time.Second)
    if err2 != nil {
        log.Fatalf("can't connect to %d: %v", s.Port, err2)
    }
    for i := 0; i < kvstorecount; i++ {
        for {
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
                time.Sleep(100*1000000) // 100ms
            } else {
                break;
            }
        }
    }
    log.Infof("waitCatchup sucess.mport:%d sport:%d", m.Port, s.Port)
}

func waitDumpBinlog(m *util.RedisServer, kvstorecount int) {
    cli, err := redis.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", m.Port), 10*time.Second)
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
    cli, err := redis.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", m.Port), 10*time.Second)
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
    cli, err := redis.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", m2.Port), 10*time.Second)
    if err != nil {
        log.Fatalf("can't connect to %d: %v", m2.Port, err)
    }

    for i := 0; i < kvstorecount; i++ {
        var binlogPos int
        if r, err := cli.Cmd("binlogpos", i).Int(); err != nil {
            log.Fatalf("do restoreBinlog %d failed:%v", i, err)
        }else {
            //log.Infof("binlogpos store:%d binlogmax:%d" , i, r)
            binlogPos = r
        }
 
        subpath := m1.Path + "/dump/" + strconv.Itoa(i) + "/";
        files, _ := filepath.Glob(subpath + "binlog*.log")
        if len(files) <= 0 {
            continue;
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
            commands = append(commands, exec.Command("../../../../../git/redis-2.8.17/src/redis-cli",
                "-p", strconv.Itoa(m2.Port)))
            pipeRun(commands)

            log.Infof("restoreBinlog sucess store:%d binlogPos:%d file:%s" , i, binlogPos, path.Base(files[j]))
        }
    }
    log.Infof("restoreBinlog sucess,port:%d" , m2.Port)
}

func shutdownServer(m *util.RedisServer) {
    cli, err := redis.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", m.Port), 10*time.Second)
    if err != nil {
        log.Fatalf("can't connect to %d: %v", m.Port, err)
    }

    if r, err := cli.Cmd("shutdown").Str(); err != nil {
        log.Fatalf("do shutdown failed:%v", err)
    } else if r != "OK" {
        log.Fatalf("do shutdown error:%s", r)
    }

    m.Destroy();
    log.Infof("shutdownServer server,port:%d", m.Port)
}

func compare(m1 *util.RedisServer, m2 *util.RedisServer) {
    cmd := exec.Command("../misc/compare_instances", fmt.Sprintf("127.0.0.1:%d", m1.Port), fmt.Sprintf("127.0.0.1:%d", m2.Port))
    cmd.Stderr = os.Stderr
    output, err := cmd.Output()
    fmt.Print("Command output:", string(output))
    if err != nil {
        fmt.Println("Command err:", err)
        log.Infof("compare failed.")
        return
    }
    log.Infof("compare sucess,port1:%d port2:%d", m1.Port, m2.Port)
}

func testRestore(m1_port int, s1_port int, s2_port int, m2_port int, kvstorecount int) {
    m1 := util.RedisServer{}
    s1 := util.RedisServer{}
    s2 := util.RedisServer{}
    m2 := util.RedisServer{}
    pwd := getCurrentDirectory()
    log.Infof("current pwd:" + pwd)
    m1.Init(m1_port, pwd, "m1_")
    s1.Init(s1_port, pwd, "s1_")
    s2.Init(s2_port, pwd, "s2_")
    m2.Init(m2_port, pwd, "m2_")

    cfgArgs := make(map[string]string)
    cfgArgs["maxBinlogKeepNum"] = "1"
    cfgArgs["kvstorecount"] = strconv.Itoa(kvstorecount)

    if err := m1.Setup(false, &cfgArgs); err != nil {
        log.Fatalf("setup master1 failed:%v", err)
    }
    if err := s1.Setup(false, &cfgArgs); err != nil {
        log.Fatalf("setup slave1 failed:%v", err)
    }
    if err := s2.Setup(false, &cfgArgs); err != nil {
        log.Fatalf("setup slave2 failed:%v", err)
    }
    if err := m2.Setup(false, &cfgArgs); err != nil {
        log.Fatalf("setup master2 failed:%v", err)
    }

    slaveof(&m1, &s1)
    slaveof(&s1, &s2)
    time.Sleep(5000*1000000) // 5s, wait slaveof success

    addData(m1_port, *num1, "aa")
    backup(&m1)
    restoreBackup(&m2)
    compare(&m1, &m2)

    waitCatchup(&m1, &s1, kvstorecount)
    waitCatchup(&m1, &s2, kvstorecount)
    compare(&m1, &s1)
    compare(&m1, &s2)

    addData(m1_port,*num2, "bb")
    addOnekeyEveryStore(&m1, kvstorecount)
    waitDumpBinlog(&m1, kvstorecount)
    flushBinlog(&m1)
    restoreBinlog(&m1, &m2, kvstorecount)
    addOnekeyEveryStore(&m2, kvstorecount)
    compare(&m1, &m2)
    
    shutdownServer(&m1);
    shutdownServer(&s1);
    shutdownServer(&s2);
    shutdownServer(&m2);
}

func main(){
    flag.Parse()
    //rand.Seed(time.Now().UTC().UnixNano())
    testRestore(*m1port, *s1port, *s2port, *m2port, *kvstorecount)
}
