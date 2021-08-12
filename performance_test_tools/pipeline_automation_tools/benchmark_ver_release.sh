#!/bin/bash

set -x

log=benchmark-$(date +"%Y%m%d%H%M%S").log
logInfo() {
    time=`date +"%Y/%m/%d %H:%M:%S"`
    echo "${time} $1" >> ${log}
}

benchmarkPidList=()

startTask() {
    task="$1"
    logInfo "${tendisVersionLongFormat} task begin: ${task}"
    resultPath="$2"
    logInfo "${task} resultPath: ${resultPath}"
    mkdir -p ${resultPath}
    dataSize="$3"
    logInfo "dataSize: ${dataSize}"
    pipelineNum="$4"
    logInfo "pipelineNum: ${pipelineNum}"
    for((i=0;i<$predixyNum;i++)); do
        ip=${ipArray[$i]}
        cmdPrefix="${benchmarkBinary} -s ${ip} -p ${port} -a ${password} -o ${resultPath}/${i} -c ${clientNum} -t ${threadNum} --test-time=${testTime} --pipeline=${pipelineNum} --distinct-client-seed --randomize --data-size=${dataSize} --random-data --key-minimum=1 --key-maximum=${keyMax}"
        if [[ "${task}" == "set" ]]; then
            ${cmdPrefix} --command='set __key__ __data__' --key-prefix='kv_' &
        elif [[ "${task}" == "get" ]]; then
            ${cmdPrefix} --command='get __key__' --key-prefix='kv_' &
        elif [[ "${task}" == "incr" ]]; then
            ${cmdPrefix} --command='incr __key__' --key-prefix='int_' &
        elif [[ "${task}" == "lpush" ]]; then
            ${cmdPrefix} --command='lpush __key__ __data__' --key-prefix='list_' &
        elif [[ "${task}" == "sadd" ]]; then
            ${cmdPrefix} --command='sadd __key__ __data__' --key-prefix='set_' &
        elif [[ "${task}" == "zadd" ]]; then
            ${cmdPrefix} --command='zadd __key__ __key__ __data__' --key-prefix='' &
        elif [[ "${task}" == "hset" ]]; then
            ${cmdPrefix} --command='hset __key__ __data__ __data__' --key-prefix='hash_' &
        fi
        benchmarkPidList+=($!)
    done
}

waitFinish() {
    for pid in ${benchmarkPidList[@]}; do
        wait $pid
        echo "$pid done"
    done
    logInfo "${tendisVersionLongFormat} task finished: $1"
    benchmarkPidList=()
}

outputReport() {
    echo "$1" >> ${mailfile}
}

runTest() {
    # $1 testType   pipeline|valuesize|lowload|multicmd|longtime test type
    # $2 cmdList            set,get,incr                         set cmd type
    # $3 valueSizeList      16,64,128    set date size used in memtier_benchmark
    # $4 testTime (second)  1800         every round test duration
    testType=$1
    cmdList=$2
    valueSizeList=$3
    testTime=$4

    mailfile=Report-$(date +"%Y%m%d%H%M%S").txt
    initTimeStamp=$(date +%s)
    if [[ $testType == ${LONGTIMETEST} ]]; then
        clientNum=50
        threadNum=20
        pipelineNum=1
        # enough for 100 days & 500k qps
        keyMax=5000000000000
    elif [[ $testType == ${MULTICMDTEST} ]]; then
        clientNum=50
        threadNum=20
        pipelineNum=1
        keyMax=5000000000
    elif [[ $testType == ${PIPELINETEST} ]]; then
        clientNum=2
        threadNum=2
        pipelineNum=50
        keyMax=5000000000
    elif [[ $testType == ${VALUESIZETEST} ]]; then
        clientNum=50
        threadNum=20
        pipelineNum=1
        keyMax=5000000000
    elif [[ $testType == ${LOWLOADTEST} ]]; then
        clientNum=15
        threadNum=10
        pipelineNum=1
        keyMax=5000000000
    fi
    for valueSize in $(echo $valueSizeList | tr ',' '\n'); do
        for cmd in $(echo $cmdList | tr ',' '\n'); do
            if [[ "$cmd" != "get" ]]; then
                ./redis-cli -h ${targetHost} -p ${port} -a ${password} flushall
                sleep ${interTime}
            fi
            startTimestamp=$(date +%s)
            resultPath="result/tmp-${startTimestamp}"
            startTask ${cmd} ${resultPath} ${valueSize} ${pipelineNum}
            waitFinish ${cmd}
            endTimestamp=$(date +%s)

            AVG=0.1 # avoid divided by zero
            P50=0.1
            P99=0.1
            P100=0.1

            for f in $(ls ${resultPath}); do
                tmpAVG=$(cat ${resultPath}/$f | grep -i ${cmd}s | tail -n 1 | awk '{print $3}')
                if [[ ! -z $tmpAVG ]]; then
                    if [ 1 -eq "$(echo "${tmpAVG} > ${AVG}" | bc)" ]; then
                        AVG=${tmpAVG}
                    fi
                fi
                tmpP50=$(cat ${resultPath}/$f | grep -i ${cmd}s | tail -n 1 | awk '{print $4}')
                if [[ ! -z $tmpP50 ]]; then
                    if [ 1 -eq "$(echo "${tmpP50} > ${P50}" | bc)" ]; then
                        P50=${tmpP50}
                    fi
                fi
                tmpP99=$(cat ${resultPath}/$f | grep -i ${cmd}s | tail -n 1 | awk '{print $5}')
                if [[ ! -z $tmpP99 ]]; then
                    if [ 1 -eq "$(echo "${tmpP99} > ${P99}" | bc)" ]; then
                        P99=${tmpP99}
                    fi
                fi
                tmpP100=$(cat ${resultPath}/$f | grep -i ${cmd}s | tail -n 1 | awk '{print $6}')
                if [[ ! -z $tmpP100 ]]; then
                    if [ 1 -eq "$(echo "${tmpP100} > ${P100}" | bc)" ]; then
                        P100=${tmpP100}
                    fi
                fi
            done
            let duration=${endTimestamp}-${startTimestamp}
            qps=$(curl -g "http://${prometheusURL}/api/v1/query?query=sum(increase(redis_command_call_duration_seconds_count{gcs_app=\"${appname}\",gcs_cluster=\"${tendisVersionShortFormat}\",gcs_dbrole=\"master\",cmd=\"${cmd}\"}[${duration}s]))by(cmd)&time=${endTimestamp}" 2>/dev/null | tr "\"" " " | awk '{print $(NF-1)}')
            qps=$(echo $qps / $duration | bc -l)
            if [[ $qps == '0' || $qps == '0.0' ]]; then
                qps=0.1 # avoid divided by zero
            fi
            decreaseLimit=''
            if [[ "$cmd" == "set" ]]; then
                decreaseLimit=${decreaseLimitSet}
                outputReport "测试命令(${predixyNum}个): ${benchmarkBinary} -c ${clientNum} -t ${threadNum} --test-time=${testTime} --pipeline=${pipelineNum} --distinct-client-seed --randomize --data-size=${dataSize} --random-data --key-minimum=1 --key-maximum=${keyMax} --command='set __key__ __data__' --key-prefix='kv_'"
            elif [[ "$cmd" == "get" ]]; then
                decreaseLimit=${decreaseLimitGet}
                outputReport "测试命令(${predixyNum}个): ${benchmarkBinary} -c ${clientNum} -t ${threadNum} --test-time=${testTime} --pipeline=${pipelineNum} --distinct-client-seed --randomize --data-size=${valueSize} --random-data --key-minimum=1 --key-maximum=${keyMax} --command='get __key__' --key-prefix='kv_'"
            elif [[ "$cmd" == "incr" ]]; then
                decreaseLimit=${decreaseLimitIncr}
                outputReport "测试命令(${predixyNum}个): ${benchmarkBinary} -c ${clientNum} -t ${threadNum} --test-time=${testTime} --pipeline=${pipelineNum} --distinct-client-seed --randomize --data-size=${valueSize} --random-data --key-minimum=1 --key-maximum=${keyMax} --command='incr __key__' --key-prefix='int_'"
            elif [[ "$cmd" == "lpush" ]]; then
                decreaseLimit=${decreaseLimitLpush}
                outputReport "测试命令(${predixyNum}个): ${benchmarkBinary} -c ${clientNum} -t ${threadNum} --test-time=${testTime} --pipeline=${pipelineNum} --distinct-client-seed --randomize --data-size=${valueSize} --random-data --key-minimum=1 --key-maximum=${keyMax} --command='lpush __key__ __data__' --key-prefix='list_'"
            elif [[ "$cmd" == "sadd" ]]; then
                decreaseLimit=${decreaseLimitSadd}
                outputReport "测试命令(${predixyNum}个): ${benchmarkBinary} -c ${clientNum} -t ${threadNum} --test-time=${testTime} --pipeline=${pipelineNum} --distinct-client-seed --randomize --data-size=${valueSize} --random-data --key-minimum=1 --key-maximum=${keyMax} --command='sadd __key__ __data__' --key-prefix='set_'"
            elif [[ "$cmd" == "zadd" ]]; then
                decreaseLimit=${decreaseLimitZadd}
                outputReport "测试命令(${predixyNum}个): ${benchmarkBinary} -c ${clientNum} -t ${threadNum} --test-time=${testTime} --pipeline=${pipelineNum} --distinct-client-seed --randomize --data-size=${valueSize} --random-data --key-minimum=1 --key-maximum=${keyMax} --command='zadd __key__ __key__ __data__' --key-prefix=''"
            elif [[ "$cmd" == "hset" ]]; then
                decreaseLimit=${decreaseLimitHset}
                outputReport "测试命令(${predixyNum}个): ${benchmarkBinary} -c ${clientNum} -t ${threadNum} --test-time=${testTime} --pipeline=${pipelineNum} --distinct-client-seed --randomize --data-size=${valueSize} --random-data --key-minimum=1 --key-maximum=${keyMax} --command='hset __key__ __data__ __data__' --key-prefix='hash_'"
            fi
            outputReport "${cmd}测试曲线：<a href=\"${grafanaURL}${tendisVersionShortFormat}&from=${startTimestamp}000&to=${endTimestamp}000\">${grafanaURL}${tendisVersionShortFormat}&from=${startTimestamp}000&to=${endTimestamp}000</a>"
            if [[ ${qps} == "0.1" ||
                  ${P50} == "0.1" ||
                  ${P99} == "0.1" ||
                  ${P100} == "0.1" ||
                  ${AVG} == "0.1" ]]; then
                continue
            fi
            if [[ $testType == ${MULTICMDTEST} ]]; then
                compareToHistory=1
            else
                compareToHistory=0
                shouldSave=0
            fi
            python writeTag.py ${cmd} ${tendisVersionShortFormat} $(date +%Y%m%d) ${qps} ${P50} ${P99} ${P100} ${AVG} ${mailfile} ${decreaseLimit} ${decreaseLimitP50} ${decreaseLimitP99} ${decreaseLimitP100} ${decreaseLimitPavg} ${shouldSave} ${compareToHistory} ${baselineVersion}
            sleep $interTime
        done
    done
    finalTimeStamp=$(date +%s)
    grafanaStartTimestamp=${initTimeStamp}000
    grafanaEndTimestamp=${finalTimeStamp}000
    logInfo "${tendisVersionLongFormat} grafanaStartTimestamp:$grafanaStartTimestamp grafanaEndTimestamp:$grafanaEndTimestamp"
    mv ${mailfile} ${mailfile}.bak
    if [[ $testType == ${LONGTIMETEST} ]]; then
        let runningTime=${finalTimeStamp}-${initTimeStamp}
        prettyFormat=$(date -d@${runningTime} -u +%H:%M:%S)
        outputReport "长时间测试，时长(hh:mm:ss)为：${prettyFormat}"
    elif [[ $testType == ${MULTICMDTEST} ]]; then
        outputReport "常见命令测试，命令列表为：${cmdList}"
    elif [[ $testType == ${PIPELINETEST} ]]; then
        let totalClient=${clientNum}*${threadNum}
        outputReport "针对特定Pipeline数，pipeline:${pipelineNum} client总数：${totalClient}"
    elif [[ $testType == ${VALUESIZETEST} ]]; then
        outputReport "针对特定Value大小测试，valueSizeList:${valueSizeList}"
    elif [[ $testType == ${LOWLOADTEST} ]]; then
        let totalClient=${clientNum}*${threadNum}
        outputReport "低负载延迟测试，client总数：${totalClient}"
    fi
    outputReport "全过程监控<a href=\"${grafanaURL}${tendisVersionShortFormat}&from=${grafanaStartTimestamp}&to=${grafanaEndTimestamp}\">${grafanaURL}${tendisVersionShortFormat}&from=${grafanaStartTimestamp}&to=${grafanaEndTimestamp}</a>"
    cat ${mailfile}.bak >> ${mailfile}
    rm ${mailfile}.bak
    if [[ $testType == ${LONGTIMETEST} ]]; then
        emailTitleSubfix="-长时间"
    elif [[ $testType == ${MULTICMDTEST} ]]; then
        emailTitleSubfix="-常见命令"
    elif [[ $testType == ${PIPELINETEST} ]]; then
        emailTitleSubfix="-特定Pipeline"
    elif [[ $testType == ${VALUESIZETEST} ]]; then
        emailTitleSubfix="-特定Value"
    elif [[ $testType == ${LOWLOADTEST} ]]; then
        emailTitleSubfix="-低负载延迟"
    fi
    /data/Anaconda2/bin/python sendmail.py ${tendisVersionLongFormat}${emailTitleSubfix} ${mailfile} ${passid} ${token} ${mailURL} ${mailSender} ${sendmailgroup}
    mkdir -p mail
    mv ${mailfile} mail/
}

outputUsage() {
    echo "usage:"
    echo "    nohup ./benchmark_ver_release.sh tendisVersionLongFormat [testType, cmdList, valueSizeList, testTime]..."
    echo "example:"
    echo "    nohup ./benchmark_ver_release.sh tendisplus-2.4.0-rocksdb-v5.13.4-test multicmd set,get,sadd 64,1024 1800 pipeline set 128 1800 &"
    echo ""
    echo "  testType: must be one of pipeline|valuesize|lowload|multicmd|longtime"
    echo "    pipeline: test with pipeline 50"
    echo "    valuesize: compare performance with different valueSize"
    echo "    lowload: test with lower load than other type"
    echo "    multicmd: test many cmd with normal load"
    echo "    longtime: test with very long time"
    echo "  cmdList can be one or more commands, with comma connected"
    echo "  valueSizeList can be one or more commands, with comma connected"
    echo "  testTime should be explicit one integer."
    echo "  ENV SAVETORESULTDB (1 or 0) used to set if save test result to resultdb"
}

main() {
    logInfo "========start========"

    # get Tendis version
    tendisVersionLongFormat=$1
    tendisVersionShortFormat=$(echo $1 | perl -p -e 's/\.//g' | perl -p -e 's/tendisplus-/cd/g' | perl -p -e 's/-rocksdb-/r/g' | perl -p -e 's/v//g' | perl -p -e 's/-test/test/g')
    shift
    logInfo "start tendisVersion: ${tendisVersionLongFormat} benchmark"
    shouldSave=${SAVETORESULTDB}
    if [[ "$shouldSave" == "1" ]]; then
        echo "should save result."
        shouldSave=1
    else
        echo "not save result"
        shouldSave=0
    fi
    logInfo "shoule test result be saved? $shouldSave"

    # source configure file
    if [ ! -f ./conf.sh ]; then
        echo "we need conf.sh"
        exit 2
    fi
    source ./conf.sh
    baselineVersion=$1
    shift
    sendGroup="sendmailgroup"$1
    eval "sendmailgroup=\$$sendGroup"
    logInfo "send mail group:"$sendmailgroup
    shift

    # kill long time benchmark started last time.
    ps aux | grep ./memtier_benchmark | grep -v grep | grep ${user} | grep test-time=8640000 | awk '{print $2}' | xargs kill -9
    sleep 10

    # get direct ip of all 3 predixy
    ipArray=()
    while [[ "1" == "1" ]]
    do
        tIP=$(getent hosts ${targetHost} | awk '{print $1}')
        ipArray+=(${tIP})
        ipArray=($(awk -v RS=' ' '!a[$1]++' <<< ${ipArray[@]}))
        if [[ "${#ipArray[@]}" == "3" ]]; then
            break
        fi
        sleep 1
    done

    # define default settings
    benchmarkBinary=./memtier_benchmark
    predixyNum=3
    interTime=300

    decreaseLimitSet=10
    decreaseLimitGet=10
    decreaseLimitIncr=10
    decreaseLimitLpush=10
    decreaseLimitSadd=10
    decreaseLimitZadd=10
    decreaseLimitHset=10
    decreaseLimitP50=50
    decreaseLimitP99=50
    decreaseLimitP100=50
    decreaseLimitPavg=50

    LONGTIMETEST="longtime"
    MULTICMDTEST="multicmd"
    VALUESIZETEST="valuesize"
    PIPELINETEST="pipeline"
    LOWLOADTEST="lowload"

    # parse command line args
    while [[ $# -gt 0 ]]; do
        if [[ $# -lt 4 ]]; then
            outputUsage
            exit 1
        fi
        testType=$1
        cmdList=$2
        valueSizeList=$3
        testTime=$4
        if [[ $testType != ${LONGTIMETEST} &&
              $testType != ${MULTICMDTEST} &&
              $testType != ${VALUESIZETEST} &&
              $testType != ${LOWLOADTEST} &&
              $testType != ${PIPELINETEST} ]]; then
            echo "unknown test type: "${testType}
            exit 1;
        fi
        shift
        shift
        shift
        shift
        echo "runTest $testType $cmdList $valueSizeList $testTime"
        runTest $testType $cmdList $valueSizeList $testTime
    done

    logInfo "end tendisVersion: $tendisVersionLongFormat benchmark"
    logInfo "========end========"
}

main $@
