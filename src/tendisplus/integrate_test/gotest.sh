#!/bin/bash

# usage:
#     ./gotest.sh [all, normaltest, normaltest-part1, normaltest-part2, normaltest-part3]

testcontent="all"
if [[ $# -ge 1 ]]; then
    testcontent=$1
    shift
fi

valid_options=("all" "normaltest" "normaltest-part1" "normaltest-part2" "normaltest-part3" "versiontest")

is_valid=false
for opt in "${valid_options[@]}"; do
    if [[ "$testcontent" == "$opt" ]]; then
        is_valid=true
        break
    fi
done

if ! $is_valid; then
    echo "Invalid argument: $testcontent"
    echo "Usage: $0 [all, normaltest, normaltest-part1, normaltest-part2, normaltest-part3, versiontest]"
    exit 1
fi
echo testcontent: $testcontent

logfile=${testcontent}.log
tmplog=./${testcontent}_tmp.log

rm -f $logfile

export GO111MODULE=on
export GOPATH=`pwd`/gopath

go get integrate_test/util
go mod download 

echo $GOPATH

export PATH=$PATH:`pwd`/../../../build/bin:`pwd`/../../../bin
function lm_traverse_dir(){
    for file in `ls $1`
    do
        if [ -d $1"/"$file ]
        then
            lm_traverse_dir $1"/"$file
        else
            file_name=$1"/"$file
            echo "===== $file_name ====="
            cat $file_name
            rm -rf $file_name
        fi
    done
}

function runOne() {
    rm $tmplog
    if [[ "${testcontent}" == "versiontest" ]]; then
        cmd=$@
        ./clear.sh ${testcontent}
    else
        cmd=$1
        ./clear.sh
    fi
    echo "" >> $logfile
    echo "###### $cmd begin ######" >> $logfile

    $cmd >> $tmplog 2>&1
    cat $tmplog
    cat $tmplog >> $logfile

    lm_traverse_dir running

    passcnt=`grep "go passed" $tmplog|wc -l`
    if [ $passcnt -lt 1 ]; then
        echo grep 'go passed' failed
        echo "##### $cmd execute failed, no find passed in $tmplog"
        exit 1
    fi
}

function checkPassed(){
    echo checkPassed begin, logfile:$1 testNum:$2
    passNum=`grep "go passed" $1 |wc -l`
    echo testNum: $2 passNum: $passNum

    if [[ $passNum -ne $2 ]]; then
        grep "\[error\]|\[fatal\]" $1

        grep "go passed" $1
        exit $passNum
    fi
    echo checkPassed succes, logfile:$1 testNum:$2
    return 0
}

testNum=0
if [[ $testcontent == "all" || "${testcontent}" == "versiontest" ]]; then
    testNum=1
    rm -rf versiontest
    go build versiontest.go common.go common_cluster.go
    runOne "./versiontest $@"
    checkPassed $logfile $testNum
fi

if [[ $testcontent == "all" || "${testcontent}" == "normaltest" || "${testcontent}" == "normaltest-part1" ]]; then
    rm -rf adminHeartbeat repl repltest restore restoretest clustertest
    go build adminHeartbeat.go common.go common_cluster.go
    go build repl.go common.go
    go build repltest.go common.go
    go build restore.go common.go
    go build restoretest.go common.go
    go build clustertest.go common.go common_cluster.go
    testNum=6

    runOne ./adminHeartbeat
    runOne ./repl
    runOne ./repltest
    runOne ./restore
    runOne ./restoretest
    runOne './clustertest -optype=set -clusterNodeNum=5 -num1=10000'
    #runOne './clustertest -optype=sadd -clusterNodeNum=5 -num1=10000'
    #runOne './clustertest -optype=hset -clusterNodeNum=5 -num1=10000'
    #runOne './clustertest -optype=lpush -clusterNodeNum=5 -num1=10000'
    #runOne './clustertest -optype=zadd -clusterNodeNum=5 -num1=10000'
    checkPassed $logfile $testNum
fi

if [[ $testcontent == "all" || "${testcontent}" == "normaltest" || "${testcontent}" == "normaltest-part2" ]]; then
    rm -rf clustertestRestore clustertestFailover dts/dts
    testNum=3
    go build clustertestRestore.go common.go common_cluster.go
    go build clustertestFailover.go common.go common_cluster.go
    go build -o dts/dts dts/dts.go dts/dts_common.go
    runOne './clustertestRestore'
    runOne './clustertestFailover'
    runOne './dts/dts'
    checkPassed $logfile $testNum
fi

if [[ $testcontent == "all" || "${testcontent}" == "normaltest" || "${testcontent}" == "normaltest-part3" ]]; then
    rm -rf dts/dts_sync deletefilesinrange memorylimit pubsubtest
    testNum=4
    go build -o dts/dts_sync dts/dts_sync.go dts/dts_common.go
    go build deletefilesinrange.go common.go common_cluster.go
    go build memorylimit.go common.go
    go build ./pubsubtest.go common.go common_cluster.go
    runOne './dts/dts_sync'
    runOne './deletefilesinrange -optype=set'
    runOne ./memorylimit
    runOne './pubsubtest'
    checkPassed $logfile $testNum
fi

