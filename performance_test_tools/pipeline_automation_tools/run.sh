#! /bin/bash

baselineVersion=2.7.0
RunTask() {
    testType=$1
    cmdList=$2
    valueSizeList=$3
    testTime=$4
    pTaskId=$(date +%s)

    ./benchmark_ver_release.sh ${tendisVersionLongFormat} ${baselineVersion} $testType $cmdList $valueSizeList $testTime $pTaskId
}

RunTask multicmd set,get,incr,lpush,sadd,zadd,hset 128 7200
RunTask lowload set 128 7200
RunTask valuesize set 128,1024 7200
RunTask pipeline set 128 7200
RunTask longtime set 128 86400

