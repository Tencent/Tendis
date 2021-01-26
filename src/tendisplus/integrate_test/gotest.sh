#!/bin/bash
logfile="gotest.log"

rm -f $logfile

srcroot=`pwd`/../../../
govendor=`pwd`/../../thirdparty/govendor/
export GOPATH=$srcroot:$govendor
echo $GOPATH
go build repl.go
go build repltest.go common.go
go build restore.go common.go
go build restoretest.go common.go
go build clustertest.go common.go common_cluster.go
go build clustertestRestore.go common.go common_cluster.go

function runOne() {
    tmplog=./gotest_tmp.log
    rm $tmplog
    cmd=$1
    ./clear.sh
    echo "" >> $logfile
    echo "###### $cmd begin ######" >> $logfile

    $cmd >> $tmplog 2>&1
    cat $tmplog
    cat $tmplog >> $logfile

    passcnt=`grep "go passed" $tmplog|wc -l`
    if [ $passcnt -lt 1 ]; then
        echo grep 'go passed' failed
        exit 1
    fi
}

runOne ./repl
runOne ./repltest
runOne ./restore
runOne ./restoretest
runOne './clustertest -benchtype=set -clusterNodeNum=5 -num1=10000'
#runOne './clustertest -benchtype=sadd -clusterNodeNum=5 -num1=10000'
#runOne './clustertest -benchtype=hmset -clusterNodeNum=5 -num1=10000'
#runOne './clustertest -benchtype=rpush -clusterNodeNum=5 -num1=10000'
#runOne './clustertest -benchtype=zadd -clusterNodeNum=5 -num1=10000'
runOne './clustertestRestore -benchtype=set'

grep "go passed" $logfile
grep -E "\[error\]|\[fatal\]" $logfile
