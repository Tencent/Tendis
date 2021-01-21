package util

import (
	"errors"
	"fmt"
	"github.com/ngaut/log"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type RedisServer struct {
	Port int
	Path string
	Ip   string
	binPath string
}


var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStrAlpha(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

// NOTE(takenliu):net.Dial failed not mean port is usable.
func FindAvailablePort(start int) int {
    time.Sleep(2 * time.Second) // wait last process listen port finish
    log.Infof("findAvailablePort begin start:%+v", start)
    for i := start; i < start+1024; i++ {
        // NOTE(takenliu):cluster port +10000
        //   use netstat to check TCP TIME_WAIT and so on
        //   use lsof to check listening port
        log.Infof("check port:%d", i)
        cmd1 := fmt.Sprintf("netstat -anpl 2>&1|grep %d", i)
        output1, err1 := exec.Command("sh", "-c", cmd1).CombinedOutput()
        log.Infof("output1:%s", string(output1))
        log.Infof("err1:%v", err1)

        cmd2 := fmt.Sprintf("netstat -anpl 2>&1|grep %d", i + 10000)
        output2, err2 := exec.Command("sh", "-c", cmd2).CombinedOutput()
        log.Infof("output2:%s", string(output2))
        log.Infof("err2:%v", err2)

        cmd3 := fmt.Sprintf("lsof -i:%d", i)
        output3, err3 := exec.Command("sh", "-c", cmd3).CombinedOutput()
        log.Infof("output3:%s", string(output3))
        log.Infof("err3:%v", err3)

        cmd4 := fmt.Sprintf("lsof -i:%d", i + 10000)
        output4, err4 := exec.Command("sh", "-c", cmd4).CombinedOutput()
        log.Infof("output4:%s", string(output4))
        log.Infof("err4:%v", err4)

        if len(output1) == 0 && len(output2) == 0 && len(output3) == 0 && len(output4) == 0 {
            log.Infof("findAvailablePort success port:%+v", i)
            return i
        }
    }
    fmt.Println("Can't find a non busy port in the $start-[expr {$start+1023}] range.")
    return 0
}

func FileExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

type CheckFuncType func(string, time.Duration) (int, error)

func CheckPidFile(pidPath string, timeout time.Duration) (int, error) {
	done := make(chan int, 1)
	go func() {
		for {
			pidBuf, err := ioutil.ReadFile(pidPath)
			if err != nil {
				time.Sleep(time.Millisecond * 200)
				continue
			}
			pid, _ := strconv.Atoi(strings.Trim(string(pidBuf), "\n"))
			exist, _ := FileExist(fmt.Sprintf("/proc/%d", pid))
			if !exist {
				log.Errorf("/proc/%d not exists", pid)
				time.Sleep(time.Millisecond * 200)
				continue
			}
			done <- pid
			break
		}
	}()

	select {
	case pid := <-done:
		log.Debugf(" process is running pid:%d", pid)
		return pid, nil
	case <-time.After(timeout):
		log.Errorf("check process timeout pid:%s", pidPath)
		return -1, errors.New("check process timeout")
	}
}

func StartProcess(command []string, env []string, pidPath string, timeout time.Duration,
	inShell bool, checkFunc CheckFuncType) (int, error) {
	if len(command) == 0 {
		log.Errorf("null command to start !!!")
		return -1, errors.New("null command to start")
	}
	log.Infof("command:%+v, env:%+v", command, env)
	done := make(chan bool, 1)
	var cmd *exec.Cmd
	go func() {
		if inShell {
			cmd = exec.Command("/bin/sh", "-c", command[0])
			// NOTE(takenliu) set pgid = pid, then SIGKILL can kill the group process
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		} else {
			cmd = exec.Command(command[0], command[1:]...)
		}
		if len(env) > 0 {
			cmd.Env = []string{}
			for _, e := range env {
				cmd.Env = append(cmd.Env, e)
			}
		}
		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Errorf("start  process failed  [err:%s] [output:%s]", err.Error(), output)
			done <- false
		} else {
			done <- true
		}
	}()

	select {
	case success := <-done:
		if success {
			log.Debugf("start process success:%d", cmd.Process.Pid)
			// maybe the daemon process start failed ,check the pid file
			if checkFunc != nil {
				return checkFunc(pidPath, timeout)
			} else {
				return cmd.Process.Pid, nil
			}
		} else {
			return -1, errors.New("start process failed")
		}
	case <-time.After(timeout):
		// maybe the fork logic error, mongod has started already
		if checkFunc != nil {
			return checkFunc(pidPath, timeout)
		} else {
			return cmd.Process.Pid, nil
		}
		//return -1, errors.New("start process timeout")
	}
}

func eventually(fn func() error, timeout time.Duration) error {
	errCh := make(chan error, 1)
	done := make(chan struct{})
	exit := make(chan struct{})

	go func() {
		for {
			err := fn()
			// ok 会 通知 done
			if err == nil {
				close(done)
				return
			}

			select {
			case errCh <- err:
			default:
			}

			select {
			case <-exit:
				return
			case <-time.After(timeout / 100):
			}
		}
	}()

	select {
	// 返回
	case <-done:
		return nil
	case <-time.After(timeout):
		close(exit)
		select {
		case err := <-errCh:
			return err
		default:
			return fmt.Errorf("timeout after %s without an error", timeout)
		}
	}
}

func (s *RedisServer) Init(ip string, port int, pwd string, path string) {
	s.Ip = ip
	s.Port = port
	s.Path = pwd + "/" + path + RandStrAlpha(6)
}

func (s *RedisServer) Destroy() {
	os.RemoveAll(s.Path)
}

func CheckPortInUse(s *RedisServer) error {
	checkStatement := fmt.Sprintf("lsof -i:%d ", s.Port)
	output, _ := exec.Command("sh", "-c", checkStatement).CombinedOutput()
	if len(output) <= 0 {
		return fmt.Errorf("port %d not in use", s.Port)
	}
	return nil
}

func (s *RedisServer) Setup(valgrind bool, cfgArgs *map[string]string) error {
	log.Debugf("mkdir " + s.Path)
	os.MkdirAll(s.Path, os.ModePerm)
	os.MkdirAll(s.Path+"/db", os.ModePerm)
	os.MkdirAll(s.Path+"/log", os.ModePerm)
	cfgFilePath := fmt.Sprintf("%s/test.cfg", s.Path)
	cfg := "bind 127.0.0.1\n"
	cfg = cfg + fmt.Sprintf("port %d\n", s.Port)
	cfg = cfg + "loglevel debug\n"
	cfg = cfg + "minBinlogKeepSec 0\n"
	cfg = cfg + fmt.Sprintf("logdir %s/log\n", s.Path)
	cfg = cfg + fmt.Sprintf("storage rocks\n")
	cfg = cfg + fmt.Sprintf("dir %s/db\n", s.Path)
	cfg = cfg + fmt.Sprintf("dumpdir %s/dump\n", s.Path)
	cfg = cfg + "rocks.blockcachemb 4096\n"
	cfg = cfg + fmt.Sprintf("pidfile %s/tendisplus.pid\n", s.Path)
	if cfgArgs != nil {
		for arg := range *cfgArgs {
			cfg = cfg + fmt.Sprintf("%s %s\n", arg, (*cfgArgs)[arg])
		}
	}

	if err := ioutil.WriteFile(cfgFilePath, []byte(cfg), 0600); err != nil {
		return err
	}

	args := []string{}
	if valgrind {
		log.Infof("start by valgrind %d", s.Port)
		// NOTE(takenliu) cmd cant be multi line.
		cmd := fmt.Sprintf("nohup ./valgrind --tool=memcheck --leak-check=full ../../../build/bin/tendisplus %s >valgrind_Tendis_%d.log 2>&1 &",
			cfgFilePath, s.Port)
		args = append(args, cmd)
		inShell := true
		StartProcess(args, []string{}, fmt.Sprintf("%s/tendisplus.pid", s.Path), 10*time.Second, inShell, CheckPidFile)
	} else {
		log.Infof("start normal %d", s.Port)
		//args = append(args, "../../../build/bin/tendisplus", cfgFilePath)
		//inShell := false
		//_, err := StartProcess(args, []string{}, fmt.Sprintf("%s/tendisplus.pid", s.Path), 10*time.Second, inShell, CheckPidFile)
		logFilePath := fmt.Sprintf("%s/stdout.log", s.Path)
		var cmd string
		binPath := "../../../build/bin/tendisplus"
		if s.binPath != "" {
			binPath = s.binPath
		}
		cmd = fmt.Sprintf("nohup %s %s > %s 2>&1 &", binPath, cfgFilePath, logFilePath)
		args := []string{}
		args = append(args, cmd)
		inShell := true
		StartProcess(args, []string{}, "", 10*time.Second, inShell, nil)
	}

	// Wait until port is in use
	err := eventually(func() error {
		return CheckPortInUse(s)
	}, 10*time.Second)
	if err != nil {
		panic(err)
	}
	return nil
}

func (s *RedisServer) Addr() string {
	return  s.Ip +":" + strconv.Itoa(s.Port)
}

func (s *RedisServer) WithBinPath(p string)  {
	s.binPath = p
}

type Predixy struct {
	RedisServer
	RedisIp   string
	RedisPort int
	Pid       int
}

func (s *Predixy) Init(ip string, port int, redisIp string, redisPort int, pwd string, path string) {
	s.Ip = ip
	s.Port = port
	s.RedisIp = redisIp
	s.RedisPort = redisPort
	s.Path = pwd + "/" + path + RandStrAlpha(6)
}

func (s *Predixy) Destroy() {
	os.RemoveAll(s.Path)
}

func (s *Predixy) Setup(valgrind bool, cfgArgs *map[string]string) error {
	log.Debugf("mkdir " + s.Path)
	os.MkdirAll(s.Path, os.ModePerm)
	cfgFilePath := fmt.Sprintf("%s/predixy.cfg", s.Path)
	logFilePath := fmt.Sprintf("%s/predixy.log", s.Path)

	cfg := "Name PredixyExample\n"
	cfg = cfg + fmt.Sprintf("Bind %s:%d\n", s.Ip, s.Port)
	cfg = cfg + "WorkerThreads 1\n"
	cfg = cfg + "ClientTimeout 300\n"
	cfg = cfg + "LogVerbSample 1\n"
	cfg = cfg + "LogDebugSample 1\n"
	cfg = cfg + "LogInfoSample 1\n"
	cfg = cfg + "LogNoticeSample 1\n"
	cfg = cfg + "LogWarnSample 1\n"
	cfg = cfg + "LogErrorSample 1\n"

	cfg = cfg + "ClusterServerPool {\n"
	cfg = cfg + "    Password tendis+test\n"
	cfg = cfg + "    MasterReadPriority 60\n"
	cfg = cfg + "    StaticSlaveReadPriority 0\n"
	cfg = cfg + "    DynamicSlaveReadPriority 0\n"
	cfg = cfg + "    RefreshInterval 1\n"
	cfg = cfg + "    ServerTimeout 1000000\n"
	cfg = cfg + "    ServerFailureLimit 100\n"
	cfg = cfg + "    ServerRetryTimeout 1000000\n"
	cfg = cfg + "    KeepAlive 120\n"
	cfg = cfg + "    Servers {\n"
	cfg = cfg + fmt.Sprintf("        + %s:%d\n", s.RedisIp, s.RedisPort)
	cfg = cfg + "    }\n"
	cfg = cfg + "}\n"

	if cfgArgs != nil {
		for arg := range *cfgArgs {
			cfg = cfg + fmt.Sprintf("%s %s\n", arg, (*cfgArgs)[arg])
		}
	}

	if err := ioutil.WriteFile(cfgFilePath, []byte(cfg), 0600); err != nil {
		return err
	}

	var cmd string
	if valgrind {
		// NOTE(takenliu) cmd cant be multi line.
		cmd = fmt.Sprintf("nohup ./valgrind --tool=memcheck --leak-check=full ../../../bin/predixy %s > %s 2>&1 &",
			cfgFilePath, logFilePath)
	} else {
		cmd = fmt.Sprintf("nohup ../../../bin/predixy %s > %s 2>&1 &", cfgFilePath, logFilePath)
	}
	args := []string{}
	args = append(args, cmd)
	inShell := true
	pid, err := StartProcess(args, []string{}, "", 10*time.Second, inShell, nil)
	s.Pid = pid

	// Wait until port is in use
	err = eventually(func() error {
		return CheckPortInUse(&s.RedisServer)
	}, 10*time.Second)
	if err != nil {
		panic(err)
	}
	return err
}
