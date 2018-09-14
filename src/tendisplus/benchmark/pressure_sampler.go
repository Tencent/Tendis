package main

import (
	"fmt"
	"bytes"
	"os"
	"os/exec"
	"time"
	"github.com/ngaut/log"
    "github.com/shirou/gopsutil/cpu"
    "github.com/shirou/gopsutil/disk"
    "github.com/shirou/gopsutil/process"
	"path/filepath"
	"strings"
	"strconv"
	"io/ioutil"
)

type Command struct {
	cmd     string
	params  []string
	tag     string
	clean   bool
	cmd_obj *exec.Cmd
}

type RocksMem struct {
	NumImm int64
    Fp     int64
	Cp     int64
    SizeAct int64
    SizeAll int64
    EstRead int64
}

func (r *RocksMem) Refresh() {
	bytes, err := ioutil.ReadFile("/tmp/rocks_main")
	if err != nil {
		log.Errorf("read rocks_main failed:%v", err)
	}
	s := string(bytes)
	s = strings.TrimSpace(s)
	s_lst := strings.Split(s, " ")
	if len(s_lst) != 6 {
		log.Errorf("invalid rocks_main line:%s", s)
		return
	}
	r.NumImm, _ = strconv.ParseInt(s_lst[0], 10, 64)
	r.Fp, _ = strconv.ParseInt(s_lst[1], 10, 64)
    r.Cp, _ = strconv.ParseInt(s_lst[2], 10, 64)
    r.SizeAct, _ = strconv.ParseInt(s_lst[3], 10, 64)
	r.SizeAll, _ = strconv.ParseInt(s_lst[4], 10, 64)
	r.EstRead, _ = strconv.ParseInt(s_lst[5], 10, 64)
}

type Recorder struct {
	file *os.File
	pid int
}

func (r *Recorder) Init(output string, pid int) {
	if tmp, err := os.Create(output); err != nil {
		log.Fatalf("create file:%s failed:%v", output, err)
	} else {
		r.file = tmp
	}
	r.pid = pid
}

func trimIO(m map[string]disk.DiskIOCountersStat) map[string]disk.DiskIOCountersStat{
	r := make(map[string]disk.DiskIOCountersStat)
	for k, v := range m {
		if k == "nvme0n1" || k == "nvme1n1" || k == "nvme2n1" || k == "nvme3n1" {
			r[k] = v
		}
	}
	return r
}

func DirSize(path string) (int64, error) {
    var size int64
    err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
        if !info.IsDir() {
            size += info.Size()
        }
        return err
    })
    return size, err
}

func (r *Recorder) Iter() {
	c, _ := cpu.CPUPercent(1000*time.Millisecond, false)
	// currently only support pressure tool in the same dir with monitor
	p, _ := process.NewProcess(int32(r.pid))
	m, _ := p.MemoryInfo()
	io1, _ := disk.DiskIOCounters()
	io1 = trimIO(io1)
	time.Sleep(1 * time.Second)
	io2, _ := disk.DiskIOCounters()
	io2 = trimIO(io2)
	if len(io1) != 4 || len(io2) != 4 {
		log.Fatalf("bad iostat:%v %v", io1, io2)
	}
	read := uint64(0)
	readbyte := uint64(0)
	write := uint64(0)
	writebyte := uint64(0)
	for _, io := range io2 {
		write += io.WriteCount
		read += io.ReadCount
		writebyte += io.WriteBytes
		readbyte += io.ReadBytes
	}
	for _, io := range io1 {
		write -= io.WriteCount
		read -= io.ReadCount
		writebyte -= io.WriteBytes
		readbyte -= io.ReadBytes
	}
	// cpu, memory, rb, wb, r, w, disk
	mem := uint64(0)
	if m != nil {
		mem = m.RSS
	} else {
		mem = 0
	}
	rocks_mem := &RocksMem{}
	rocks_mem.Refresh()

	s := fmt.Sprintf("%d %d %d %d %d %d %d 0 %d %d %d %d %d %d\n", time.Now().Unix(), int(c[0]*48), mem,
		readbyte, writebyte, read, write, rocks_mem.NumImm, rocks_mem.Fp, rocks_mem.Cp, rocks_mem.SizeAct, rocks_mem.SizeAll, rocks_mem.EstRead)
	if _, err := r.file.WriteString(s); err != nil {
		log.Fatalf("write %s to file failed:%v", s, err)
	}
}

func (r *Recorder) Summery() {
	dirsize, err := DirSize("./")
	if err != nil {
		log.Fatalf("dirsize calc err:%v", err)
	}
	s := fmt.Sprintf("0 0 0 0 0 0 0 %d 0 0 0 0 0 0\n", dirsize)
	if _, err := r.file.WriteString(s); err != nil {
		log.Fatalf("write %s to file failed:%v", s, err)
	}

}

func (r *Recorder) Close() {
	r.file.Close()
}

var cmd_list []*Command = nil

func doMonitor(cmd *Command) {
	recorder := &Recorder{}
	recorder.Init(cmd.tag, cmd.cmd_obj.Process.Pid)
	defer func() {
		recorder.Summery()
		recorder.Close()
	}()
	done_chan := make(chan int)
	go func() {
		if err := cmd.cmd_obj.Wait(); err != nil {
			log.Errorf("cmd:%v wait failed:%v", cmd.cmd_obj, err)
		}
		close(done_chan)
	}()
	for {
		select {
		case <-done_chan:
			log.Infof("cmd:%v process done", cmd.cmd_obj)
			return
		case <-time.After(1 * time.Second):
			recorder.Iter()
		}
	}
}

//std::cout<< "./bin [load/run] db_num cf_num thd_num memtable_num memtable_size record_num" << std::endl;
func main() {
	cmd_list = []*Command{
		&Command{
			cmd:    "./test",
			params: []string{"odb", "load", "100", "1", "40", "4", "67108864", "1000000000"},
			tag:    "odb_load_100_1_40_4_64MB_1GB",
			clean:  true,
		},
		&Command{
			cmd:    "./test",
			params: []string{"odb", "run", "100", "1", "40", "4", "67108864", "1000000000"},
			tag:    "odb_run_100_1_40_4_64MB_1GB",
			clean:  false,
		},
		&Command{
			cmd:    "./test",
			params: []string{"odb", "load", "1", "100", "40", "4", "67108864", "1000000000"},
			tag:    "odb_load_1_100_40_4_64MB_1GB",
			clean:  true,
		},
		&Command{
			cmd:    "./test",
			params: []string{"odb", "run", "1", "100", "40", "4", "67108864", "1000000000"},
			tag:    "odb_run_1_100_40_4_64MB_1GB",
			clean:  false,
		},
		&Command{
			cmd:    "./test",
			params: []string{"pdb", "load", "100", "1", "40", "4", "67108864", "1000000000"},
			tag:    "pdb_load_100_1_40_4_64MB_1GB",
			clean:  true,
		},
		&Command{
			cmd:    "./test",
			params: []string{"pdb", "run", "100", "1", "40", "4", "67108864", "1000000000"},
			tag:    "pdb_run_100_1_40_4_64MB_1GB",
			clean:  false,
		},
		&Command{
			cmd:    "./test",
			params: []string{"pdb", "load", "1", "100", "40", "4", "67108864", "1000000000"},
			tag:    "pdb_load_1_100_40_4_64MB_1GB",
			clean:  true,
		},
		&Command{
			cmd:    "./test",
			params: []string{"pdb", "run", "1", "100", "40", "4", "67108864", "1000000000"},
			tag:    "pdb_run_1_100_40_4_64MB_1GB",
			clean:  false,
		},
	}
	for _, c := range cmd_list {
		if c.clean {
			if err := os.RemoveAll("./db"); err != nil {
				log.Fatalf("rmdir db failed:%v", err)
			}
			if err := os.MkdirAll("./db", os.ModePerm); err != nil {
				log.Fatalf("rmdir failed:%v", err)
			}
			log.Infof("rmdir and mkdir for %s", c.tag)
		}
		log.Infof("begin expreiment %s", c.tag)
		cmd := exec.Command(c.cmd, c.params...)
		var err_out bytes.Buffer
		cmd.Stderr = &err_out
		if err := cmd.Start(); err != nil {
			log.Errorf("cmd:%v start failed:%v", cmd, err)
			continue
		}
		c.cmd_obj = cmd
		doMonitor(c)
	}
}
