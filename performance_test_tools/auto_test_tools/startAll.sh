source ./taskList.sh
source ./conf.sh

curDir=`pwd`

function startQpsSh() {
    cd $dir
    qps=`ps axu |grep qps.sh|grep -v "grep"|wc -l`
    if [ $qps == 0 ]
    then
        nohup ./qps.sh &
    fi
    cd $curDir
}

function stopBenchmark() {
    ps axu |grep memtier_benchmark |grep $USER |awk '{print $2}'|xargs kill -9 >/dev/null 2>&1
}

function stopServer() {
    $redisClient -h $ip -p $port $passwdString shutdown
    sleep 5
    ps axu |grep $bin |grep $USER |grep $confFile|awk '{print $2}'|xargs kill -9 >/dev/null 2>&1
    # be carefull, dont use "rm $data_dir/ -rf",
    #     if $data_dir is null will cause "rm / -rf"
    if $clearData
    then
        echo clearData
        rm $dir/$data_dir/db/ -rf
        rm $dir/$data_dir/dump/ -rf
        rm $dir/$data_dir/log/ -rf
        rm $dir/$data_dir/tendisplus.pid
    fi
    sleep 5
    return
}

function startServer() {
    cd $dir
    mkdir $data_dir/db/ -p
    mkdir $data_dir/dump/ -p
    mkdir $data_dir/log/ -p

    executorNum=$1
    sed -i "s/executorthreadnum.*/executorthreadnum $executorNum/g" $confFile
    let coreNo=$coreNum-1
    echo taskset -c 0-$coreNo $bin $confFile
    taskset -c 0-$coreNo $bin $confFile
    sleep 5
    
    if $clusterEnable
    then
        $redisClient -h $ip -p $port $passwdString cluster addslots {0..16383}
    fi
    sleep 5

    cd $curDir
    return
}

function benchMark() {
    executorNum=$1
    cmd=$2
    valueLen=$3
    benchCmd="$benchDir/memtier_benchmark -t 20 -c 50 -s $ip -p $port --distinct-client-seed"
    if [ $cmd == "set" ]
    then
        cmdStr="--command=\"set __key__ __data__\" --key-prefix=\"kv_\" --command-key-pattern=R --random-data --data-size=$valueLen"
    elif [ $cmd == "get" ]
    then
        cmdStr="--command=\"get __key__\" --key-prefix=\"kv_\" --command-key-pattern=R"
    elif [ $cmd == "incr" ]
    then
        cmdStr="--command=\"incr __key__\" --key-prefix=\"int_\" --key-minimum=1 --key-maximum=1000000"
    elif [ $cmd == "lpush" ]
    then
        #cmdStr="--command=\"lpush mylist __data__\" --random-data --data-size=$valueLen"
        cmdStr="--command=\"lpush __key__ __data__\" --key-prefix=\"list_\" --key-minimum=1 --key-maximum=1000000 --random-data --data-size=$valueLen"
    elif [ $cmd == "sadd" ]
    then
        #cmdStr="--command=\"sadd myset __data__\" --random-data --data-size=$valueLen"
        cmdStr="--command=\"sadd __key__ __data__\" --key-prefix=\"set_\" --key-minimum=1 --key-maximum=1000000 --random-data --data-size=$valueLen"
    elif [ $cmd == "zadd" ]
    then
        #cmdStr="--command=\"zadd myzset __key__ __data__\" --key-prefix=\"\" --key-minimum=1 --key-maximum=100000000 --random-data --data-size=$valueLen"
        # $valueLen is useless
        cmdStr="--command=\"zadd __key__ __key__ __data__\" --key-prefix=\"\" --key-minimum=1 --key-maximum=1000000 --random-data --data-size=128"
    elif [ $cmd == "hset" ]
    then
        #cmdStr="--command=\"hset myhash __key__ __data__\" --command-key-pattern=R --random-data --data-size=$valueLen"
        # $valueLen is useless
        cmdStr="--command=\"hset __key__ __data__ __data__\" --key-prefix=\"hash_\" --key-minimum=1 --key-maximum=1000000 --random-data --data-size=128"
    else
        echo unkown cmd:$cmd
        return
    fi

    # benchmark first time, the latency is very high
    echo bechmark preheat: $benchCmd $cmdStr --test-time=2
    eval $benchCmd $cmdStr --test-time=2

    echo bechmark start: $benchCmd $cmdStr --test-time=$benchTime
    eval $benchCmd $cmdStr --test-time=$benchTime> memtier_${executorNum}_${cmd}_${valueLen}.log

    cp $dir/qps.log qps_${executorNum}_${cmd}_${valueLen}.log

    return
}

echo ================================================
echo `date "+%Y-%m-%d %H:%M:%S"` startAll.sh begin

ulimit -n 100000
ulimit -c unlimited

startQpsSh
stopBenchmark

#rm qps_*.log
#rm memtier_*.log
time=`date "+%Y-%m-%d_%H:%M:%S"`
mmv qps_\* ${time}_qps_\#1
mmv memtier_\* ${time}_memtier_\#1

firstValueLen=${valueLenList[0]}
for executorNum in ${executorNumList[@]}
do
    for valueLen in ${valueLenList[@]}
    do
        needClear=true
        for cmd in ${cmdList[@]}
        do
            echo
            if [ $cmd == "noClear" ]
            then
                needClear=false
                continue
            fi
            echo `date "+%Y-%m-%d %H:%M:%S"` begin task executorNum:$executorNum valueLen:$valueLen cmd:$cmd
            if $needClear
            then
                echo stopServer and startServer
                stopServer
                startServer $executorNum
            fi
            needClear=true
            
            if [[ $valueLen != $firstValueLen && $cmd != "set" && $cmd != "get" ]]
            then
                echo ignore task executorNum:$executorNum valueLen:$valueLen cmd:$cmd
                continue
            fi

            echo benchMark $executorNum $valueLen $cmd $needClear
            benchMark $executorNum $cmd $valueLen
            echo `date "+%Y-%m-%d %H:%M:%S"` finish task executorNum:$executorNum valueLen:$valueLen cmd:$cmd
        done
    done
done

echo `date "+%Y-%m-%d %H:%M:%S"` startAll.sh end
echo
