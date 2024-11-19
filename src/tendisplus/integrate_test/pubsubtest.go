// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

package main

import (
	"errors"
	"flag"
	"integrate_test/util"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/mediocregopher/radix.v2/pubsub"

	"github.com/ngaut/log"
)

func publishData(channel string, limit int, master *util.RedisServer) {
	master_cli := createClient(master)
	for i := 0; i < limit; i++ {
		re := master_cli.Cmd("publish", channel, i)
		if re.Err != nil {
			log.Fatalf("AddData fail %s", re.Err.Error())
		}
	}
}

func check_publish_data(Sub_cli *pubsub.SubClient, limit int) bool {
	check := 0
	for { //get all receive message
		re := Sub_cli.Receive()
		if re.Timeout() { //no more message could receive will timeout
			break
		}
		reply, er := re.Array() //check the message isn't correct
		if er != nil {
			log.Fatalf("Check message fail:%s", er)
		}
		replyMessage, err := reply[2].Int()
		if err != nil {
			log.Fatalf("Error message type:%s", reply[2])
		}
		if replyMessage >= limit || replyMessage < 0 {
			log.Fatalf("Error message appear:%s", reply[2])
		}
		if replyMessage != check {
			log.Fatalf("Error elements: expect: %d get: %d", check, replyMessage)
		}
		check++
	}
	if check < limit { //check isn't missing message
		log.Fatalf("Missing elements: %d/%d", check, limit)
		return false
	}
	return true
}

func check_sub_and_pub(master *util.RedisServer, channel string) (bool, error) {
	limit := 2000
	Sub_cli := pubsub.NewSubClient(createClient(master))
	_, err := Sub_cli.Subscribe(channel).Array()
	if err != nil {
		return false, err
	}
	publishData(channel, limit, master)
	return check_publish_data(Sub_cli, limit), nil
}

func check_connection_is_close(Sub_cli *pubsub.SubClient) bool {
	re := Sub_cli.Ping()
	return re.Err.Error() == "EOF"
}

type smartSubCli struct {
	channel string
	file    string
}

func useSmartClient(Port int, isAuth bool, pwd string, Cmd ...string) *exec.Cmd {
	Args := []string{}
	Args = append(Args, "-c")
	Args = append(Args, "-p")
	Args = append(Args, strconv.Itoa(Port))
	if isAuth {
		Args = append(Args, "-a")
		Args = append(Args, pwd)
	}
	Args = append(Args, Cmd...)
	return exec.Command("../../../bin/redis-cli", Args...)
}

func setOutput(cmd *exec.Cmd, file string) {
	f, _ := os.OpenFile(file, os.O_WRONLY|os.O_CREATE, 0777)
	cmd.Stdout = f
	cmd.Stderr = f
}

func (c smartSubCli) getSubscribeResult() string {
	time.Sleep(20 * time.Millisecond)

	content, err := os.ReadFile(c.file)
	if err != nil {
		panic(err)
	}
	os.Truncate(c.file, 0)
	return string(content)
}

func smart_subscribe(Port int, isAuth bool, pwd string, Channel string) (smartSubCli, error) {
	Path := util.GetCurrentDirectory() + "/" + "subscribe"
	file := Path + "/" + Channel + "_" + time.Now().String() + ".log"
	os.MkdirAll(Path, os.ModePerm)
	os.OpenFile(file, os.O_WRONLY|os.O_CREATE, 0777)

	Args := []string{"subscribe"}
	Args = append(Args, Channel)
	cmd := useSmartClient(Port, isAuth, pwd, Args...)
	setOutput(cmd, file)
	cmd.Start()

	c := smartSubCli{Channel, file}
	reply := c.getSubscribeResult()
	_, err := check_subscribe(reply, Channel)
	return c, err
}

func smart_publish(Port int, isAuth bool, pwd string, Channel string, message string) (string, error) {
	Args := []string{"publish"}
	Args = append(Args, Channel)
	Args = append(Args, message)
	res, err := useSmartClient(Port, isAuth, pwd, Args...).CombinedOutput()
	return string(res), err
}

func getMessage(mess string, channel string) []string {
	res := strings.Split(mess, "message\n")
	var val []string
	for v := range res {
		if strings.HasPrefix(res[v], channel) {
			val = append(val, strings.Trim(res[v], channel+"\n"))
		}
	}
	return val
}

func check_subscribe(mess string, channel string) (bool, error) {
	if strings.Contains(mess, "subscribe\n"+channel) {
		return true, nil
	}
	return false, errors.New(mess)
}

func smartPublishData(channel string, limit int, master *util.RedisServer) {
	for i := 0; i < limit; i++ {
		_, err := smart_publish(master.Port, true, "tendis+test", channel, strconv.Itoa(i))
		if err != nil {
			log.Fatalf("AddData fail %s", err.Error())
		}
	}
}

func smart_check_sub_and_pub(master *util.RedisServer, channel string) (bool, error) {
	limit := 2000
	Sub_cli, err := smart_subscribe(master.Port, true, "tendis+test", channel)
	if err != nil {
		return false, err
	}
	smartPublishData(channel, limit, master)
	return smart_check_publish_data(Sub_cli, limit), nil
}
func smart_check_publish_data(c smartSubCli, limit int) bool {
	reply := getMessage(c.getSubscribeResult(), c.channel)
	check := 0
	for i := range reply { //check the message isn't correct
		replyMessage, err := strconv.Atoi(reply[i])
		if err != nil {
			log.Fatalf("Unexpected message type:%s", reply[i])
		}
		if replyMessage >= limit || replyMessage < 0 {
			log.Fatalf("Error message appear:%s", reply[i])
		}
		if replyMessage != check {
			log.Fatalf("Error elements, expect: %d get: %d", check, replyMessage)
		}
		check++
	}
	if check < limit { //check isn't missing message
		log.Fatalf("Missing elements: %d/%d", check, limit)
		return false
	}
	return true
}

func basicTest(master util.RedisServer, isUseSmartRedisCli bool, channel string) {
	log.Infof("start basic subscribe test")
	var check bool
	var err error
	if isUseSmartRedisCli {
		check, err = smart_check_sub_and_pub(&master, channel)
	} else {
		check, err = check_sub_and_pub(&master, channel)
	}
	if check {
		log.Infof("basic subscribe test susscess")
	} else {
		log.Fatalf("basic subscribe test fail:%s", err)
	}
}

func movedTest(wrongMaster util.RedisServer, channel string) {
	//MOVED test tendis could return MOVED when receive a pubsub cmd which slot doesn't match
	log.Infof("start MOVED test")
	wrong_sub_cli := pubsub.NewSubClient(createClient(&wrongMaster))
	_, err := wrong_sub_cli.Subscribe(channel).Array() //subscribe the wrong server

	if strings.HasPrefix(err.Error(), "MOVED") { //if slot not match,tendis should return moved
		log.Infof("MOVED test susscess")
	} else {
		log.Fatalf("MOVED test fail %s", err.Error())
	}
}

func migrateTest(predixy util.RedisServer, servers *[]util.RedisServer, clusterNodeNum int, NodeInfo *[]NodeInfo, isConnectToProxy bool, isUseSmartRedisCli bool) {
	channel := "migratetest"
	slot := get_slot(channel, clusterNodeNum, (*servers)[0])
	node := get_node_by_slot(slot, NodeInfo)
	var master util.RedisServer
	if isConnectToProxy {
		master = predixy
	} else {
		master = (*servers)[node]
	}

	log.Infof("start migrate test")
	dst_master := (*servers)[(node+1)%clusterNodeNum]
	dst_cli := createClient(&dst_master)
	before_sub_cli := pubsub.NewSubClient(createClient(&master))
	var before_smart_cli smartSubCli
	var err error
	var check bool

	if isUseSmartRedisCli {
		before_smart_cli, err = smart_subscribe(master.Port, true, "tendis+test", channel)
	} else {
		_, err = before_sub_cli.Subscribe(channel).Array() //tendis should close all connection which match the migrate slot when migrate
	}
	if err != nil { //check is subscribe success
		log.Fatalf("subscribe fail %s", err)
	}
	//start migrate
	dst_cli.Cmd("Cluster", "setslot", "importing", getNodeName(&(*servers)[node]), slot) //after failover, the slave should be the new master

	if isUseSmartRedisCli {
		time.Sleep(200 * time.Millisecond) //wait for migrate end
		//tendis should close all connection which match the migrate slot when migrate
		closed := strings.HasSuffix(before_smart_cli.getSubscribeResult(), "Error: Server closed the connection\n")
		if !closed {
			log.Fatalf("connection should close after migrate")
		}
		check, err = smart_check_sub_and_pub(&master, channel) //smart redis-cli could handle moved, so it should success
	} else {
		wait := time.Now().Add(15 * time.Second)
		for {
			time.Sleep(20 * time.Millisecond)
			if time.Now().After(wait) {
				log.Fatalf("connection didn't close after migrate")
			}
			if check_connection_is_close(before_sub_cli) { //when the connection closed,the migrate should be finish
				check, err = check_sub_and_pub(&master, channel) //now migrate is finish but predixy doesn't get migrate info from cluster info
				break                                            //predixy will get moved from tendis, predixy should handle move itself
			}
		}
		if isConnectToProxy {
			if !check { //predixy should handle the moved message and rediret to new master
				log.Fatalf("predixy subscribe fail %s", err.Error())
			}
		} else {
			if !strings.HasPrefix(err.Error(), "MOVED") { //now the dst_master should handle the slot, subscribe should fail and return moved to dst_master
				log.Fatalf("subscribe cmd should moved after migrate %s", err.Error())
			}
			master = dst_master
			check, err = check_sub_and_pub(&master, channel) //check could get message after migrate
		}
	}
	if check {
		log.Infof("migrate test susscess")
	} else {
		log.Fatalf("migrate test fail %s", err.Error())
	}
}

func failoverTest(predixy util.RedisServer, servers *[]util.RedisServer, clusterNodeNum int, NodeInfo *[]NodeInfo, isConnectToProxy bool, isUseSmartRedisCli bool, isManual bool) util.RedisServer {
	var test string
	if isManual {
		test = "manual"
	} else {
		test = "shutdown"
	}
	channel := test + "failovertest"
	slot := get_slot(channel, clusterNodeNum, (*servers)[0])
	node := get_node_by_slot(slot, NodeInfo)
	var master util.RedisServer
	if isConnectToProxy {
		master = predixy
	} else {
		master = (*servers)[node]
	}

	log.Infof("start %s failover test", test)
	slave := (*servers)[node+clusterNodeNum]
	var before_smart_cli smartSubCli
	var before_sub_cli *pubsub.SubClient
	var err error
	var check bool

	if isUseSmartRedisCli {
		before_smart_cli, err = smart_subscribe(master.Port, true, "tendis+test", channel)
	} else {
		before_sub_cli = pubsub.NewSubClient(createClient(&master))
		_, err = before_sub_cli.Subscribe(channel).Array() //tendis should close all connection when failover
	}
	if err != nil {
		log.Fatalf("subscribe fail %s", err)
	}

	oldmaster := (*servers)[node]
	if isManual {
		cluster_manual_failover(&slave)
		time.Sleep(5 * time.Second) //wait for failover end
	} else {
		createClient(&oldmaster).Cmd("shutdown")
		time.Sleep(25 * time.Second) //wait for failover end
	}
	if !isConnectToProxy { //after failover the slave should be new master
		master = slave
	}
	if isUseSmartRedisCli {
		res := before_smart_cli.getSubscribeResult()
		if !strings.HasSuffix(res, "Error: Server closed the connection\n") { //tendis should close all connection when failover
			log.Fatalf("connection should close after %s failover", test)
		}
		check, err = smart_check_sub_and_pub(&master, channel) //check could get message after failover
	} else {
		if !check_connection_is_close(before_sub_cli) { //the connection should be close
			log.Fatalf("connection should close after failover")
		}
		if isConnectToProxy {
			check, err = check_sub_and_pub(&master, channel) //check could get message after failover
		} else {
			if isManual {
				_, err = check_sub_and_pub(&oldmaster, channel) //now tendis should reply moved message point to new master
				if !strings.HasPrefix(err.Error(), "MOVED") {   //now the old master should be a slave, subscribe should fail and return moved to new master
					log.Fatalf("subscribe cmd should moved after failover %s", err.Error())
				}
			}
			check, err = check_sub_and_pub(&master, channel) //check could get message after failover
		}
	}
	if check {
		log.Infof("%s failover test susscess", test)
	} else {
		log.Fatalf("%s failover test fail: %s", test, err.Error())
	}
	return oldmaster
}

func testClusterPubSub(mport int, clusterNodeNum int, isConnectToProxy bool, isUseSmartRedisCli bool) {
	cfg := make(map[string]string)
	cfg["enable-move-pubsub-request"] = "true"
	cfg["enable-close-pubsub-connection"] = "true"
	var servers, predixy, NodeInfo = startCluster("127.0.0.1", mport, clusterNodeNum, cfg, util.GetCurrentDirectory(), "")
	log.Infof("ClusterPubSubtest begin isConnectToProxy: %t isUseSmartRedisCli: %t", isConnectToProxy, isUseSmartRedisCli)

	channel := "pubsubtest"
	slot := get_slot(channel, clusterNodeNum, (*servers)[0])
	node := get_node_by_slot(slot, NodeInfo)
	var master util.RedisServer
	if isConnectToProxy {
		master = predixy.RedisServer
	} else {
		master = (*servers)[node]
	}

	basicTest(master, isUseSmartRedisCli, channel)
	movedTest((*servers)[(node+1)%clusterNodeNum], channel)
	migrateTest(predixy.RedisServer, servers, clusterNodeNum, NodeInfo, isConnectToProxy, isUseSmartRedisCli)
	failoverTest(predixy.RedisServer, servers, clusterNodeNum, NodeInfo, isConnectToProxy, isUseSmartRedisCli, true)
	failMaster := failoverTest(predixy.RedisServer, servers, clusterNodeNum, NodeInfo, isConnectToProxy, isUseSmartRedisCli, false)
	//failmaster shutdown on shutdownfailover test so it couldn't be shutdown twice
	log.Infof("ClusterPubSubtest pass")
	for i := 0; i < clusterNodeNum*2; i++ {
		if failMaster == (*servers)[i] {
			continue
		}
		shutdownServer(&(*servers)[i], *shutdown, *clear)
	}
	shutdownPredixy(predixy, *shutdown, *clear)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	flag.Parse()
	log.Infof("pubsubtest.go start.")

	testClusterPubSub(45290, 3, false, false)
	testClusterPubSub(45300, 3, true, false)
	testClusterPubSub(45290, 3, false, true)
	testClusterPubSub(45300, 3, true, true)

	log.Infof("pubsubtest.go passed.")
}
