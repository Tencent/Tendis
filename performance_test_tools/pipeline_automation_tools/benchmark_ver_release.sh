#!/bin/bash

set -x

log=benchmark-$(date +"%Y%m%d").log
logInfo() {
    time=`date +"%Y/%m/%d %H:%M:%S"`
    echo "${time} $1" >> ${log}
}

benchmarkPidList=()
extract_master_ips() {
    # 使用redis-cli命令获取服务器信息，然后提取master的IP
    ./redis-cli -h ${targetHost} -p ${port} -a ${password} info servers | awk '
        /^Server:/ {
            server = substr($0, 8)
        }
        /^Role:master$/ {
            print server
        }
    '
}
flushAll() {
    masters=($(extract_master_ips))
    for master in "${masters[@]}"; do
        ip="${master%:*}"
        redisport="${master##*:}"
        logInfo "cleanall: $ip $redisport"
        ./redis-cli -h $ip -p ${redisport} -a ${redispsw} cleanall
    done
}


startTask() {
    task="$1"
    logInfo "${tendisVersionLongFormat} task begin: ${task}"
    resultPath="$2"
    logInfo "${task} resultPath: ${resultPath}"
    dataSize="$3"
    logInfo "dataSize: ${dataSize}"
    pipelineNum="$4"
    logInfo "pipelineNum: ${pipelineNum}"

    # get direct ip of all 3 predixy
    predixyFile=predixy.txt
    if [ ${#ipArray[@]} -eq 0 ] && [ -f "$predixyFile" ]; then
        mapfile -t ipArray < $predixyFile
    fi
    while [[ "${#ipArray[@]}" != "${predixyNum}" ]]
    do
        tIP=$(getent hosts ${targetHost} | awk '{print $1}')
        ipArray+=(${tIP})
        ipArray=($(awk -v RS=' ' '!a[$1]++' <<< ${ipArray[@]}))
        sleep 1
    done
    logInfo "predixy list:${ipArray[@]}"
    printf "%s\n" "${ipArray[@]}" > $predixyFile

    # define default settings
    benchmarkBinary=./memtier_benchmark

    killall $benchmarkBinary

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
    pTaskId=$5

    mailfile=Report-${pTaskId}.txt
    rm $mailfile

    logInfo "$testType $cmdList $valueSizeList $testTime $pTaskId $mailfile"

    parentResultPath="result/tmp-${pTaskId}/"
    mkdir -p $parentResultPath
    parentInfoFile=$parentResultPath/info.txt

    edgeExpandTime=180
    initTimeStamp=$(($(date +%s) - $edgeExpandTime))
    if [ ! -f "$parentInfoFile" ]; then
        echo $initTimeStamp >> $parentInfoFile
    else
        initTimeStamp=`head -1 $parentInfoFile`
    fi

    if [[ $testType == ${LONGTIMETEST} ]]; then
        clientNum=10
        threadNum=10
        pipelineNum=100
        keyMax=50000000000
    elif [[ $testType == ${MULTICMDTEST} ]]; then
        clientNum=10
        threadNum=10
        pipelineNum=100
        keyMax=5000000000
    elif [[ $testType == ${PIPELINETEST} ]]; then
        clientNum=2
        threadNum=2
        pipelineNum=50
        keyMax=5000000000
    elif [[ $testType == ${VALUESIZETEST} ]]; then
        clientNum=25
        threadNum=5
        pipelineNum=1
        keyMax=5000000000
    elif [[ $testType == ${LOWLOADTEST} ]]; then
        clientNum=15
        threadNum=10
        pipelineNum=1
        keyMax=5000000000
    fi
    interTime=300

    predixyNum=3
    # get direct ip of all 3 predixy
    ipArray=()

    hasMultiTask=0
    if echo "$valueSizeList" | grep -q ',' || echo "$cmdList" | grep -q ','; then
        hasMultiTask=1;
    fi
    for valueSize in $(echo $valueSizeList | tr ',' '\n'); do
        for cmd in $(echo $cmdList | tr ',' '\n'); do
            resultPath="${parentResultPath}/${cmd}-${valueSize}"
            infoFile=$resultPath/info.txt
            if [ ! -d $resultPath ]; then
                mkdir -p ${resultPath}

                if [[ "$cmd" != "get" ]]; then
                    flushAll
                fi
                sleep ${interTime}

                startTimestamp=$(($(date +%s) - $edgeExpandTime))
                echo $startTimestamp >> $infoFile
 
                startTask ${cmd} ${resultPath} ${valueSize} ${pipelineNum}
                waitFinish ${cmd}

                endTimestamp=$(($(date +%s) + $edgeExpandTime))
                echo $endTimestamp >> $infoFile
            fi
            startTimestamp=`head -1 $infoFile`
            endTimestamp=`tail -1 $infoFile`
            qps=0.1
            AVG=0.1 # avoid divided by zero
            P50=0.1
            P99=0.1
            P100=0.1

            for f in $(ls ${resultPath}); do
		
                tmpQps=$(cat ${resultPath}/$f | grep -i ${cmd}s | tail -n 1 | awk '{print $2}')
		if [[ ! -z $tmpQps ]]; then
		    qps=$(echo "$qps + $tmpQps" | bc)
		fi
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
            # getQPS ${startTimestamp} ${endTimestamp} ${tendisVersionShortFormat} ${cmd}
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

            decreaseLimit=''

            outputReport "<b>$cmd命令:</b>"
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
            outputReport "${cmd}测试曲线:<br> <a href=\"${grafanaURL}&from=${startTimestamp}000&to=${endTimestamp}000\">${grafanaURL}&from=${startTimestamp}000&to=${endTimestamp}000</a>"
            python3 getRenderPicture.py ${parentResultPath} $renderUrl $pngUrl $bk_app_code $bk_app_secret $bk_username $bk_biz_id $dashboard_uid $panel_id $app $cluster_domain ${startTimestamp} ${endTimestamp}
            python3 addPicture.py "${parentResultPath}/${startTimestamp}-${endTimestamp}.jpeg" ${mailfile}
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
            python3 writeTag.py ${cmd} ${tendisVersionShortFormat} $(date +%Y%m%d) ${qps} ${P50} ${P99} ${P100} ${AVG} ${mailfile} ${decreaseLimit} ${decreaseLimitP50} ${decreaseLimitP99} ${decreaseLimitP100} ${decreaseLimitPavg} ${shouldSave} ${compareToHistory} ${baselineVersion}
        done
    done
    finalTimeStamp=$(($(date +%s) + $edgeExpandTime))
    if [ $(wc -l < "$parentInfoFile") -eq 2 ]; then
        finalTimeStamp=`tail -1 $parentInfoFile`
    else
        echo $finalTimeStamp >> $parentInfoFile
    fi

    initTimeStampMs=${initTimeStamp}000
    finalTimeStampMs=${finalTimeStamp}000
    logInfo "${tendisVersionLongFormat} initTimeStampMs:$initTimeStampMs finalTimeStampMs:$finalTimeStampMs"
    mv ${mailfile} ${mailfile}.bak
    if [[ $testType == ${LONGTIMETEST} ]]; then
        let runningTime=${finalTimeStamp}-${initTimeStamp}
        days=$((runningTime / 86400))
        remaining=$((runningTime % 86400))
        hours=$((remaining / 3600))
        remaining=$((remaining % 3600))
        minutes=$((remaining / 60))
        seconds=$((remaining % 60))
        prettyFormat=$(printf "%02dd:%02dh:%02dm:%02ds" $days $hours $minutes $seconds)

        outputReport "长时间测试，时长为：${prettyFormat}"
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

    if [ "$hasMultiTask" -eq 1 ]; then
        outputReport "<b>全过程监控:</b><br> <a href=\"${grafanaURL}&from=${initTimeStampMs}&to=${finalTimeStampMs}\">${grafanaURL}&from=${initTimeStampMs}&to=${finalTimeStampMs}</a>"
        python3 getRenderPicture.py ${parentResultPath} $renderUrl $pngUrl $bk_app_code $bk_app_secret $bk_username $bk_biz_id $dashboard_uid $panel_id $app $cluster_domain ${initTimeStamp} ${finalTimeStamp}
        python3 addPicture.py "${parentResultPath}/${initTimeStamp}-${finalTimeStamp}.jpeg" ${mailfile}
    fi

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
    python3 sendmail.py ${tendisVersionLongFormat}${emailTitleSubfix} ${mailfile} ${passid} ${token} ${mailURL} ${mailSender} ${sendmailgroup}
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
    tendisVersionShortFormat=${tendisVersionLongFormat#*-} && tendisVersionShortFormat=${tendisVersionShortFormat%%-*}
    shift
        # source configure file
    if [ ! -f ./conf.sh ]; then
        echo "we need conf.sh"
        exit 2
    fi
    source ./conf.sh
    shouldSave=${SAVETORESULTDB}
    if [[ "$shouldSave" == "1" ]]; then
        echo "should save result."
        shouldSave=1
    else
        echo "not save result"
        shouldSave=0
    fi

    baselineVersion=$1
    shift

    logInfo "start task: ${tendisVersionLongFormat} ${baselineVersion} shouldSave: ${shouldSave}"

    LONGTIMETEST="longtime"
    MULTICMDTEST="multicmd"
    VALUESIZETEST="valuesize"
    PIPELINETEST="pipeline"
    LOWLOADTEST="lowload"

    # parse command line args
    while [[ $# -gt 0 ]]; do
        if [[ $# -lt 5 ]]; then
            outputUsage
            exit 1
        fi
        testType=$1
        cmdList=$2
        valueSizeList=$3
        testTime=$4
        pTaskId=$5

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
        shift
        echo "runTest $testType $cmdList $valueSizeList $testTime"
        runTest $testType $cmdList $valueSizeList $testTime $pTaskId
    done

    logInfo "end task"
    logInfo "=========end========="
}

main $@
