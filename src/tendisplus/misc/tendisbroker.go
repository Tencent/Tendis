// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/ngaut/log"
	"sync"
	"time"
)

var (
	backend     = flag.String("backendhost", "9.24.0.133:12345", "backend twenproxy host")
	frontenddir = flag.String("frontenddir", "./frontend", "dir contains frontend info")
)

const (
	KEY                = 4
	DBID               = 4
	DBIDV2             = 2
	OBJVSN             = 2
	SIZEPRE            = 1
	TYPEPRE            = 1
	OFFSET             = DBID + TYPEPRE
	REDIS_ENCODING_INT = 1

	KSET     = 0
	KDEL     = 1
	LPUSH    = 2
	RPUSH    = 3
	LPOP     = 4
	RPOP     = 5
	LSET     = 6
	HSET     = 7
	HDEL     = 8
	SADD     = 9
	SREM     = 10
	ZADD     = 11
	ZREM     = 12
	EXPIRE   = 13
	CAS      = 16
	ASYNCDEL = 17
	SYNCVSN  = 18
	RCLVSN   = 19
)

type Frontend struct {
	Host    string
	Passwd  string
	Runid   string
	LastSeq int64
	State   int32
}

type Binlog struct {
	LogType int32
	Dbid    int16
	Enc     int32
	Ttl     int64
	Ts      uint64
	Key     string
	Val     string
	Aux     string
	State   int32
}

func parseFronts(dir string) ([]*Frontend, error) {
	tmp := &Frontend{
		Host:    "9.24.0.133:10000",
		Passwd:  "tt7715TTC",
		Runid:   "276c0be4e0de6d5760a315b2e9cc4ac4984cedde",
		LastSeq: 0,
		State:   2,
	}
	return []*Frontend{tmp}, nil
}

func NM(s string) []byte {
	return []byte(s)[OFFSET:]
}

func makeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func procKSet(binlog *Binlog, c *redis.Client) error {
	key := NM(binlog.Key)
	valStr := ""
	if binlog.Enc == REDIS_ENCODING_INT {
		reader := bytes.NewReader([]byte(binlog.Val))
		val := int64(0)
		if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
			return err
		}
		valStr = fmt.Sprintf("%d", val)
	} else {
		valStr = binlog.Val
	}
	if err := c.Cmd("SELECT", binlog.Dbid).Err; err != nil {
		return err
	}
	if binlog.Ttl == 0 {
		if err := c.Cmd("SET", key, valStr).Err; err != nil {
			return err
		}
	} else {
		tm := int64(1)
		now := makeTimestamp()
		if now < binlog.Ttl {
			tm = binlog.Ttl - now
		}
		if err := c.Cmd("PSETEX", key, tm, valStr).Err; err != nil {
			return err
		}
	}
	return nil
}

func procKDel(binlog *Binlog, c *redis.Client) error {
	key := NM(binlog.Key)
	if err := c.Cmd("SELECT", binlog.Dbid).Err; err != nil {
		return err
	}
	if err := c.Cmd("DEL", key).Err; err != nil {
		return err
	}
	return nil
}

func procPush(binlog *Binlog, c *redis.Client) error {
	key := NM(binlog.Key)
	reader := bytes.NewReader(key[:4])
	keylen := int32(0)
	if err := binary.Read(reader, binary.LittleEndian, &keylen); err != nil {
		return err
	}
	key = key[4 : 4+keylen]

	valStr := ""
	if binlog.Enc == REDIS_ENCODING_INT {
		reader := bytes.NewReader([]byte(binlog.Val))
		val := int64(0)
		if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
			return err
		}
		valStr = fmt.Sprintf("%d", val)
	} else {
		valStr = binlog.Val
	}

	if err := c.Cmd("SELECT", binlog.Dbid).Err; err != nil {
		return err
	}
	if binlog.LogType == LPUSH {
		if err := c.Cmd("LPUSH", key, valStr).Err; err != nil {
			return err
		}
	} else {
		if err := c.Cmd("RPUSH", key, valStr).Err; err != nil {
			return err
		}
	}
	return nil
}

func procPop(binlog *Binlog, c *redis.Client) error {
	key := NM(binlog.Key)
	reader := bytes.NewReader(key[:4])
	keylen := int32(0)
	if err := binary.Read(reader, binary.LittleEndian, &keylen); err != nil {
		return err
	}
	key = key[4 : 4+keylen]

	if err := c.Cmd("SELECT", binlog.Dbid).Err; err != nil {
		return err
	}
	if binlog.LogType == LPOP {
		if err := c.Cmd("LPOP", key).Err; err != nil {
			return err
		}
	} else {
		if err := c.Cmd("RPOP", key).Err; err != nil {
			return err
		}
	}
	return nil
}

func procLSet(binlog *Binlog, c *redis.Client) error {
	key := NM(binlog.Key)
	valStr := ""
	reader := bytes.NewReader(key[:4])
	keylen := int32(0)
	if err := binary.Read(reader, binary.LittleEndian, &keylen); err != nil {
		return err
	}
	key = key[4 : 4+keylen]

	reader = bytes.NewReader([]byte(binlog.Aux))
	index := int64(0)
	if err := binary.Read(reader, binary.LittleEndian, &index); err != nil {
		return err
	}
	if binlog.Enc == REDIS_ENCODING_INT {
		reader := bytes.NewReader([]byte(binlog.Val))
		val := int64(0)
		if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
			return err
		}
		valStr = fmt.Sprintf("%d", val)
	} else {
		valStr = binlog.Val
	}
	if err := c.Cmd("SELECT", binlog.Dbid).Err; err != nil {
		return err
	}
	if err := c.Cmd("LSET", key, index, valStr).Err; err != nil {
		return err
	}
	return nil
}

func procHSet(binlog *Binlog, c *redis.Client) error {
	key := NM(binlog.Key)
	reader := bytes.NewReader(key[:4])
	valStr := ""
	keylen := int32(0)
	if err := binary.Read(reader, binary.LittleEndian, &keylen); err != nil {
		return err
	}

	ele := key[4+keylen:]
	key = key[4 : 4+keylen]
	if binlog.Enc == REDIS_ENCODING_INT {
		reader := bytes.NewReader([]byte(binlog.Val))
		val := int64(0)
		if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
			return err
		}
		valStr = fmt.Sprintf("%d", val)
	} else {
		valStr = binlog.Val
	}
	if err := c.Cmd("SELECT", binlog.Dbid).Err; err != nil {
		return err
	}
	if err := c.Cmd("HSET", key, ele, valStr).Err; err != nil {
		return err
	}
	return nil
}

func procHDel(binlog *Binlog, c *redis.Client) error {
	key := NM(binlog.Key)
	reader := bytes.NewReader(key[:4])
	keylen := int32(0)
	if err := binary.Read(reader, binary.LittleEndian, &keylen); err != nil {
		return err
	}

	ele := key[4+keylen:]
	key = key[4 : 4+keylen]
	if err := c.Cmd("SELECT", binlog.Dbid).Err; err != nil {
		return err
	}
	if err := c.Cmd("HDEL", key, ele).Err; err != nil {
		return err
	}
	return nil
}

func procSAR(binlog *Binlog, c *redis.Client) error {
	key := NM(binlog.Key)
	reader := bytes.NewReader(key[:4])
	keylen := int32(0)
	if err := binary.Read(reader, binary.LittleEndian, &keylen); err != nil {
		return err
	}

	ele := key[4+keylen:]
	key = key[4 : 4+keylen]
	eleStr := ""
	if binlog.Enc == REDIS_ENCODING_INT {
		reader := bytes.NewReader(ele)
		val := int64(0)
		if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
			return err
		}
		eleStr = fmt.Sprintf("%d", val)
	} else {
		eleStr = string(ele)
	}
	if err := c.Cmd("SELECT", binlog.Dbid).Err; err != nil {
		return err
	}
	if binlog.LogType == SADD {
		if err := c.Cmd("SADD", key, eleStr).Err; err != nil {
			return err
		}
	} else {
		if err := c.Cmd("SREM", key, eleStr).Err; err != nil {
			return err
		}
	}
	return nil
}

func procZadd(binlog *Binlog, c *redis.Client) error {
	key := NM(binlog.Key)
	reader := bytes.NewReader(key[:4])
	keylen := int32(0)
	if err := binary.Read(reader, binary.LittleEndian, &keylen); err != nil {
		return err
	}

	ele := key[4+keylen:]
	key = key[4 : 4+keylen]
	valStr := ""

	reader = bytes.NewReader([]byte(binlog.Val))
	val := int64(0)
	if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
		return err
	}
	valStr = fmt.Sprintf("%d", val)
	if err := c.Cmd("ZADD", key, valStr, ele).Err; err != nil {
		return err
	}
	return nil
}

func procZRem(binlog *Binlog, c *redis.Client) error {
	key := NM(binlog.Key)
	reader := bytes.NewReader(key[:4])
	keylen := int32(0)
	if err := binary.Read(reader, binary.LittleEndian, &keylen); err != nil {
		return err
	}

	ele := key[4+keylen:]
	key = key[4 : 4+keylen]

	if err := c.Cmd("SELECT", binlog.Dbid).Err; err != nil {
		return err
	}
	if err := c.Cmd("ZREM", key, ele).Err; err != nil {
		return err
	}
	return nil
}

func procExpire(binlog *Binlog, c *redis.Client) error {
	key := NM(binlog.Key)
	tm := int64(1)
	now := makeTimestamp()
	if now < binlog.Ttl {
		tm = binlog.Ttl - now
	}
	if err := c.Cmd("SELECT", binlog.Dbid).Err; err != nil {
		return err
	}
	if err := c.Cmd("PEXPIRE", key, tm).Err; err != nil {
		return err
	}
	return nil
}

func procCas(binlog *Binlog, c *redis.Client) error {
	if err := c.Cmd("SELECT", binlog.Dbid).Err; err != nil {
		return err
	}
	reader := bytes.NewReader([]byte(binlog.Val)[len(binlog.Val)-8:])
	vsn := int64(0)
	if err := binary.Read(reader, binary.LittleEndian, &vsn); err != nil {
		return err
	}

	if binlog.Key[KEY] == 'a' {
		valStr := ""
		if binlog.Enc == REDIS_ENCODING_INT {
			reader := bytes.NewReader([]byte(binlog.Val))
			val := int64(0)
			if err := binary.Read(reader, binary.LittleEndian, &val); err != nil {
				return err
			}
			valStr = fmt.Sprintf("%d", val)
		} else {
			valStr = binlog.Val
		}
		if err := c.Cmd("CAS", binlog.Key, vsn, valStr).Err; err != nil {
			return err
		}
	} else if binlog.Key[KEY] == 'H' {
		log.Fatalf("cas on hash not impl")
	}
	return nil
}

func procAsyncDel(binlog *Binlog, c *redis.Client) error {
	log.Fatalf("procAsyncDel not impl")
	return nil
}

func procSyncVer(binlog *Binlog, c *redis.Client) error {
	log.Fatalf("procSyncVer not impl")
	return nil
}

func procRecVer(binlog *Binlog, c *redis.Client) error {
	log.Fatalf("procRecVer not impl")
	return nil
}

func procBinlog(binlog *Binlog, c *redis.Client) error {
	switch binlog.LogType {
	case KSET:
		return procKSet(binlog, c)
	case KDEL:
		return procKDel(binlog, c)
	case LPUSH:
		fallthrough
	case RPUSH:
		return procPush(binlog, c)
	case LPOP:
		fallthrough
	case RPOP:
		return procPop(binlog, c)
	case LSET:
		return procLSet(binlog, c)
	case HSET:
		return procHSet(binlog, c)
	case HDEL:
		return procHDel(binlog, c)
	case SADD:
		fallthrough
	case SREM:
		return procSAR(binlog, c)
	case ZADD:
		return procZadd(binlog, c)
	case ZREM:
		return procZRem(binlog, c)
	case EXPIRE:
		return procExpire(binlog, c)
	case 14:
		fallthrough
	case 15:
		return nil
	case CAS:
		return procCas(binlog, c)
	case ASYNCDEL:
		return procAsyncDel(binlog, c)
	case SYNCVSN:
		return procSyncVer(binlog, c)
	case RCLVSN:
		return procRecVer(binlog, c)
	default:
		log.Fatalf("invalid logtype:%+v", *binlog)
	}
	return nil
}

func genBinlog(s string) (*Binlog, error) {
	result := &Binlog{}
	pos := uint64(0)
	arr := []byte(s)
	reader := bytes.NewReader(arr[pos:])
	if err := binary.Read(reader, binary.LittleEndian, &result.LogType); err != nil {
		return nil, err
	}
	pos += 4

	reader = bytes.NewReader(arr[pos:])
	if err := binary.Read(reader, binary.LittleEndian, &result.Enc); err != nil {
		return nil, err
	}
	pos += 4

	reader = bytes.NewReader(arr[pos:])
	if err := binary.Read(reader, binary.LittleEndian, &result.Ttl); err != nil {
		return nil, err
	}
	pos += 8

	reader = bytes.NewReader(arr[pos:])
	if err := binary.Read(reader, binary.LittleEndian, &result.Ts); err != nil {
		return nil, err
	}
	pos += 8

	reader = bytes.NewReader(arr[pos:])
	keylen := uint64(0)
	if err := binary.Read(reader, binary.LittleEndian, &keylen); err != nil {
		return nil, err
	}
	pos += 8
	result.Key = string(arr[pos : pos+keylen])
	pos += keylen
	reader = bytes.NewReader([]byte(result.Key))
	if err := binary.Read(reader, binary.LittleEndian, &result.Dbid); err != nil {
		return nil, err
	}

	reader = bytes.NewReader(arr[pos:])
	vallen := uint64(0)
	if err := binary.Read(reader, binary.LittleEndian, &vallen); err != nil {
		return nil, err
	}
	pos += 8

	if vallen > 0 {
		if result.Enc == REDIS_ENCODING_INT { // REDIS_ENCODING_INT
			if vallen != 8 { // sizeof(long)
				log.Fatalf("encode_int size unmatch 8")
			}
		}
		result.Val = string(arr[pos : pos+vallen])
		pos += vallen
	}

	if pos != uint64(len(arr)) {
		result.Aux = string(arr[pos:])
	}
	return result, nil
}

func front2back(front *Frontend, back_host string, wg *sync.WaitGroup) {
	defer wg.Done()
	fc, err := redis.DialTimeout("tcp", front.Host, 10*time.Second)
	if err != nil {
		log.Fatalf("dial %s failed:%v", front.Host, err)
	}
	bc, err := redis.DialTimeout("tcp", back_host, 10*time.Second)
	if err != nil {
		log.Fatalf("dial %s failed:%v", back_host, err)
	}
	if v, err := fc.Cmd("AUTH", front.Passwd).Str(); err != nil || v != "OK" {
		log.Fatalf("auth %s failed", front.Host)
	}
	state_buf := &bytes.Buffer{}
	if err := binary.Write(state_buf, binary.LittleEndian, front.State); err != nil {
		log.Fatalf("write state failed:%v", err)
	}
	seq_buf := &bytes.Buffer{}
	if err := binary.Write(seq_buf, binary.LittleEndian, front.LastSeq); err != nil {
		log.Fatalf("write lastseq failed:%v", err)
	}
	if v, err := fc.Cmd("CNYS", state_buf.Bytes(), seq_buf.Bytes(), "lastkey", front.Runid).Str(); err != nil || v != "OK" {
		log.Fatalf("cnys host:%v failed:%v %v", front.Host, v, err)
	}
	if err := fc.WriteRaw([]byte("PING")); err != nil {
		log.Fatalf("cnys oneway ping host:%v failed:%v", front.Host, err)
	}

	for {
		rsp, err := fc.ReadResp().Array()
		if err != nil {
			log.Fatalf("readraw failed:%v", err)
		}
		if len(rsp) != 4 {
			log.Fatalf("bad paste size:%d", len(rsp))
		}
		if name, err := rsp[0].Str(); err != nil {
			log.Fatalf("convert rsp[0] to str failed:%v", err)
		} else if name != "paste" {
			log.Fatalf("bad sync cmd:%s", name)
		}

		typ, err := rsp[1].Int64()
		if err != nil {
			log.Fatalf("convert rsp[1] to int failed:%v", err)
		}

		_, err = rsp[2].Str()
		if err != nil {
			log.Fatalf("convert rsp[2] to string failed:%v", err)
		}

		val, err := rsp[3].Str()
		if err != nil {
			log.Fatalf("convert rsp[3] to string failed:%v", err)
		}

		if typ == 3 {
			continue
		}

		if typ == 4 {
			log.Infof("receive flush from:%s", front.Host)
			continue
		}

		binlog, err := genBinlog(val)
		if err != nil {
			log.Fatalf("gen binlog failed:%v", err)
		} else {
			log.Infof("curr binlog %+v", *binlog)
		}

		if err := procBinlog(binlog, bc); err != nil {
			log.Fatalf("proc binlog failed: %+v, %v", *binlog, err)
		}
	}
}

func main() {
	flag.Parse()
	frontends, err := parseFronts(*frontenddir)
	if err != nil {
		log.Fatalf("parse frontenddir failed:%v", err)
	}
	var wg sync.WaitGroup
	for _, o := range frontends {
		wg.Add(1)
		go front2back(o, *backend, &wg)
	}
	wg.Wait()
}
