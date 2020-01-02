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
	"time"
)

type RedisServer struct {
	Port int
	Path string
	Ip string
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStrAlpha(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
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

func StartProcess(command []string, env []string, pidPath string, timeout time.Duration) (int, error) {
	if len(command) == 0 {
		log.Errorf("null command to start !!!")
		return -1, errors.New("null command to start")
	}
	log.Infof("command:%+v, env:%+v", command, env)
	done := make(chan bool, 1)
	go func() {
		cmd := exec.Command(command[0], command[1:]...)
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
			log.Debugf("start process success")
			// maybe the daemon process start failed ,check the pid file
			return CheckPidFile(pidPath, timeout)
		} else {
			return -1, errors.New("start process failed")
		}
	case <-time.After(timeout):
		// maybe the fork logic error, mongod has started already
		return CheckPidFile(pidPath, timeout)
		//return -1, errors.New("start process timeout")
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

func (s *RedisServer) Setup(valgrind bool, cfgArgs *map[string]string) error {
    log.Debugf("mkdir " + s.Path)
	os.MkdirAll(s.Path, os.ModePerm)
	os.MkdirAll(s.Path+"/db", os.ModePerm)
	os.MkdirAll(s.Path+"/log", os.ModePerm)
	cfgFilePath := fmt.Sprintf("%s/test.cfg", s.Path)
	cfg := "bind 127.0.0.1\n"
	cfg = cfg + fmt.Sprintf("port %d\n", s.Port)
	cfg = cfg + "loglevel debug\n"
	cfg = cfg + fmt.Sprintf("logdir %s/log\n", s.Path)
	cfg = cfg + fmt.Sprintf("storage rocks\n")
	cfg = cfg + fmt.Sprintf("dir %s/db\n", s.Path)
	cfg = cfg + fmt.Sprintf("dumpdir %s/dump\n", s.Path)
	cfg = cfg + "rocks.blockcachemb 4096\n"
	cfg = cfg + fmt.Sprintf("pidfile %s/tendisplus.pid\n", s.Path)
    if cfgArgs != nil {
        for arg := range *cfgArgs {
             cfg = cfg + fmt.Sprintf("%s %s\n",arg, (*cfgArgs)[arg])
        }
    }

	if err := ioutil.WriteFile(cfgFilePath, []byte(cfg), 0600); err != nil {
		return err
	}

	args := []string{}
	if valgrind {
		args = append(args, "./valgrind", "--tool=memcheck", "--leak-check=full",
			"../../../build/bin/tendisplus", cfgFilePath)
	} else {
		args = append(args, "../../../build/bin/tendisplus", cfgFilePath)
	}
	_, err := StartProcess(args, []string{}, fmt.Sprintf("%s/tendisplus.pid", s.Path), 10*time.Second)
	return err
}
