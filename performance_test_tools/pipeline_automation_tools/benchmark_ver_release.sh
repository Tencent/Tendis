#!/bin/bash

set -x

log=benchmark.log
logInfo() {
    time=`date +"%Y/%m/%d %H:%M:%S"`
    echo $time $1 >> $log
}

stopTask() {
    ps axu | grep $benmark_binary | grep -v grep | grep $user | grep $ipmatch | awk '{print $2}' | xargs kill -9
}

startTask() {
    stopTask

    task=$1
    logInfo "$tendisVersion task begin: $task"
    resultpath="result/tmp-$2"
    logInfo "$tast tmp result path: $resultpath"
    mkdir -p ${resultpath}
    for((i=0;i<$benchnum;i++))
    do
        ip=${IParr[$i]}
        cmdPref="$benmark_binary -t ${threadnum} -c ${clientnum} -s $ip -p $port --distinct-client-seed -a $pw --test-time=${testTime}"
        if [ "$task" = "set" ]; then
            $cmdPref --command='set __key__ __data__' --key-prefix='kv_' --key-minimum=1 --key-maximum=500000000 --random-data --data-size=128 -o ${resultpath}/${i} &
        elif [ "$task" = "get" ]; then
            $cmdPref --command='get __key__' --key-prefix='kv_' --key-minimum=1 --key-maximum=500000000 -o ${resultpath}/${i} &
        elif [ $task = "incr" ]; then
            $cmdPref --command='incr __key__' --key-prefix='int_' --key-minimum=1 --key-maximum=1000000 -o ${resultpath}/${i} &
        elif [ $task = "lpush" ]; then
            $cmdPref --command='lpush __key__ __data__' --key-prefix='list_' --key-minimum=1 --key-maximum=1000000 --random-data --data-size=128 -o ${resultpath}/${i} &
        elif [ $task = "sadd" ]; then
            $cmdPref --command='sadd __key__ __data__' --key-prefix='set_' --key-minimum=1 --key-maximum=1000000 --random-data --data-size=128 -o ${resultpath}/${i} &
        elif [ $task = "zadd" ]; then
            $cmdPref --command='zadd __key__ __key__ __data__' --key-prefix='' --key-minimum=1 --key-maximum=1000000 --random-data --data-size=128 -o ${resultpath}/${i} &
        elif [ $task = "hset" ]; then
            $cmdPref --command='hset __key__ __data__ __data__' --key-prefix='hash_' --key-minimum=1 --key-maximum=1000000 --random-data --data-size=128 -o ${resultpath}/${i} &
        fi
    done
}

waitFinish() {
    while ((1))
    do
        num=`ps axu | grep $benmark_binary | grep -v grep | grep $user | awk '{print $2}' | wc -l`
        if (($num == 0))
        then
            break
        fi
        sleep 1
    done
    logInfo "$tendisVersion task finished: $1"
}

if [[ $# != 5 ]]
then
    echo "usage: "
    echo "    nohup bash benchmark_ver_release.sh tendisVersion shortTimeTestList testTime longTimeTestList testTime &"
    echo "example: "
    echo "    nohup bash benchmark_ver_release.sh 2-3-4 set,get 30 set 1800 &"
    exit 1
fi

logInfo "========start========"
tendisVersion=$1
logInfo "start tendisVersion: $tendisVersion benchmark"
shortTimeTestList=$2
shortTestTime=$3
longTimeTestList=$4
longTestTime=$5

if [ ! -f ./k8s.sh ]
then
    echo "we need k8s.sh"
    exit 2
fi
source ./k8s.sh

benmark_binary=./memtier_benchmark

clientnum=50
threadnum=20
benchnum=3
interTime=120
defaultTestTime=600

decreaseLimit_set=10
decreaseLimit_get=10
decreaseLimit_incr=10
decreaseLimit_lpush=10
decreaseLimit_sadd=10
decreaseLimit_zadd=10
decreaseLimit_hset=10
decreaseLimit_p50=50
decreaseLimit_p99=50
decreaseLimit_p100=50
decreaseLimit_pavg=50

IParr=()
while [[ "1" == "1" ]]
do
    tIP=$(getent hosts ${targethost} | awk '{print $1}')
    IParr+=(${tIP})
    IParr=($(awk -v RS=' ' '!a[$1]++' <<< ${IParr[@]}))
    if [[ "${#IParr[@]}" == "3" ]]; then
        break
    fi
    sleep 1
done

outputReport() {
    echo "$1" >> ${mailfile}
}

runTest() {
    mailfile=Report.txt
    rm ${mailfile}
    #1e8 need about 8 minutes
    initTimeStamp=$(date +%s)
    for itest in $(echo $1 | tr ',' '\n')
    do
        testTime=${defaultTestTime}
        curTestTime=$2
        if [[ "$curTestTime" == "0" ]]
        then
            return 0
        fi
        startTimestamp=$(date +%s)
        while [[ "$curTestTime" != "0" ]]
        do
            if [[ "$curTestTime" -gt "$testTime" ]]
            then
                let curTestTime=curTestTime-testTime
            else
                testTime=${curTestTime}
                curTestTime=0
            fi
            startTask ${itest} ${startTimestamp}
            waitFinish ${itest}
        done
        testTime=${defaultTestTime}
        endTimestamp=$(date +%s)
        resultpath="result/tmp-$startTimestamp"
        AVG=0.0
        P50=0.0
        P99=0.0
        P100=0.0

        for f in $(ls ${resultpath})
        do
            tmpAVG=$(cat ${resultpath}/$f | grep -i ${itest}s | tail -n 1 | awk '{print $3}')
            if [[ ! -z $tmpAVG ]]; then
                if [ 1 -eq "$(echo "${tmpAVG} > ${AVG}" | bc)" ]; then
                    AVG=${tmpAVG}
                fi
            fi
            tmpP50=$(cat ${resultpath}/$f | grep -i ${itest}s | tail -n 1 | awk '{print $4}')
            if [[ ! -z $tmpP50 ]]; then
                if [ 1 -eq "$(echo "${tmpP50} > ${P50}" | bc)" ]; then
                    P50=${tmpP50}
                fi
            fi
            tmpP99=$(cat ${resultpath}/$f | grep -i ${itest}s | tail -n 1 | awk '{print $5}')
            if [[ ! -z $tmpP99 ]]; then
                if [ 1 -eq "$(echo "${tmpP99} > ${P99}" | bc)" ]; then
                    P99=${tmpP99}
                fi
            fi
            tmpP100=$(cat ${resultpath}/$f | grep -i ${itest}s | tail -n 1 | awk '{print $6}')
            if [[ ! -z $tmpP100 ]]; then
                if [ 1 -eq "$(echo "${tmpP100} > ${P100}" | bc)" ]; then
                    P100=${tmpP100}
                fi
            fi
        done
        let duration=${endTimestamp}-${startTimestamp}
        qps=$(curl -g "http://${prometheusURL}/api/v1/query?query=sum(increase(redis_command_call_duration_seconds_count{gcs_app=\"${appname}\",gcs_cluster=\"${clusterprefix}-${tendisVersion}\",gcs_dbrole=\"master\",cmd=\"${itest}\"}[${duration}s]))by(cmd)&time=${endTimestamp}" 2>/dev/null | tr "\"" " " | awk '{print $(NF-1)}')
        qps=$(echo $qps / $duration | bc -l)
        decreaseLimit=''
        if [[ "$itest" == "set" ]]; then
            decreaseLimit=${decreaseLimit_set}
            outputReport "测试命令(${benchnum}个): $benmark_binary -t ${threadnum} -c ${clientnum} --distinct-client-seed --test-time=${testTime} --command='set __key__ __data__' --key-prefix='kv_' --key-minimum=1 --key-maximum=500000000 --random-data --data-size=128"
        elif [[ "$itest" == "get" ]]; then
            decreaseLimit=${decreaseLimit_get}
            outputReport "测试命令(${benchnum}个): $benmark_binary -t ${threadnum} -c ${clientnum} --distinct-client-seed --test-time=${testTime} --command='get __key__' --key-prefix='kv_' --key-minimum=1 --key-maximum=500000000"
        elif [[ "$itest" == "incr" ]]; then
            decreaseLimit=${decreaseLimit_incr}
            outputReport "测试命令(${benchnum}个): $benmark_binary -t ${threadnum} -c ${clientnum} --distinct-client-seed --test-time=${testTime} --command='incr __key__' --key-prefix='int_' --key-minimum=1 --key-maximum=1000000"
        elif [[ "$itest" == "lpush" ]]; then
            decreaseLimit=${decreaseLimit_lpush}
            outputReport "测试命令(${benchnum}个): $benmark_binary -t ${threadnum} -c ${clientnum} --distinct-client-seed --test-time=${testTime} --command='lpush __key__ __data__' --key-prefix='list_' --key-minimum=1 --key-maximum=1000000 --random-data --data-size=128"
        elif [[ "$itest" == "sadd" ]]; then
            decreaseLimit=${decreaseLimit_sadd}
            outputReport "测试命令(${benchnum}个): $benmark_binary -t ${threadnum} -c ${clientnum} --distinct-client-seed --test-time=${testTime} --command='sadd __key__ __data__' --key-prefix='set_' --key-minimum=1 --key-maximum=1000000 --random-data --data-size=128"
        elif [[ "$itest" == "zadd" ]]; then
            decreaseLimit=${decreaseLimit_zadd}
            outputReport "测试命令(${benchnum}个): $benmark_binary -t ${threadnum} -c ${clientnum} --distinct-client-seed --test-time=${testTime} --command='zadd __key__ __key__ __data__' --key-prefix='' --key-minimum=1 --key-maximum=1000000 --random-data --data-size=128"
        elif [[ "$itest" == "hset" ]]; then
            decreaseLimit=${decreaseLimit_hset}
            outputReport "测试命令(${benchnum}个): $benmark_binary -t ${threadnum} -c ${clientnum} --distinct-client-seed --test-time=${testTime} --command='hset __key__ __data__ __data__' --key-prefix='hash_' --key-minimum=1 --key-maximum=1000000 --random-data --data-size=128"
        fi
        outputReport "${itest}测试曲线：<a href=\"${grafanaURL}-${tendisVersion}&from=${startTimestamp}000&to=${endTimestamp}000\">${grafanaURL}-${tendisVersion}&from=${startTimestamp}000&to=${endTimestamp}000</a>"
        python writeTag.py ${itest} ${tendisVersion} $(date +%Y%m%d) ${qps} ${P50} ${P99} ${P100} ${AVG} ${mailfile} ${decreaseLimit} ${decreaseLimit_p50} ${decreaseLimit_p99} ${decreaseLimit_p100} ${decreaseLimit_pavg}

        if [[ "$itest" != "set" ]]
        then
            ./flushdb.exp ${K8sClusterNode} ${user} ${K8sNodePW} ${K8sNodeHN} ${K8sNodePort} ${K8sNodeGrepString} ${appname}
        fi
        sleep $interTime
    done
    finalTimeStamp=$(date +%s)

    grafanaStartTimestamp=${initTimeStamp}000
    grafanaEndTimestamp=${finalTimeStamp}000
    logInfo "${tendisVersion} grafanaStartTimestamp:$grafanaStartTimestamp grafanaEndTimestamp:$grafanaEndTimestamp"
    mv ${mailfile} ${mailfile}.bak
    outputReport "全过程监控<a href=\"${grafanaURL}-${tendisVersion}&from=${grafanaStartTimestamp}&to=${grafanaEndTimestamp}\">${grafanaURL}-${tendisVersion}&from=${grafanaStartTimestamp}&to=${grafanaEndTimestamp}</a>"
    cat ${mailfile}.bak >> ${mailfile}
    rm ${mailfile}.bak
    /data/Anaconda2/bin/python sendmail.py ${tendisVersion} ${passid} ${token} ${mailURL} ${mailSender} ${sendmailgroup}
}

runTest $shortTimeTestList $shortTestTime
runTest $longTimeTestList $longTestTime

logInfo "end tendisVersion: $tendisVersion benchmark"
logInfo "========end========"
