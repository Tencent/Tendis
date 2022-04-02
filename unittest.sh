#!/bin/bash

logfile=./unittest.log
rm $logfile

function runOne() {
    tmplog=./unittest_tmp.log
    rm $tmplog
    if [ ! -f "$1" ]; then
        echo "$1 is not exist, exiting.."
        exit -1
    fi

    cmd="$1 --gtest_throw_on_failure"
    $cmd >> $tmplog 2>&1
    cat $tmplog
    cat $tmplog >> $logfile

    errcnt=`grep -E "Expected|FAILED|Check failure stack trace|core dumped|Failure|INVARIANT|heap-use-after-free|heap-buffer-overflow|stack-buffer-overflow|global-buffer-overflow|stack-use-after-return|stack-use-after-scope|initialization-order-fiasco" $logfile|wc -l`
    if [ $errcnt -ne 0 ]; then
        grep -E "Expected|FAILED|Check failure stack trace|core dumped|Failure|INVARIANT|heap-use-after-free|heap-buffer-overflow|stack-buffer-overflow|global-buffer-overflow|stack-use-after-return|stack-use-after-scope|initialization-order-fiasco" $logfile
        exit $errcnt
    fi
    #passcnt=`grep -E "\[  PASSED  \]" $tmplog|wc -l`
    #if [ $passcnt -lt 1 ]; then
    #    exit 1
    #fi
}

dir=build/bin

runOne "./$dir/record_test"
runOne "./$dir/lock_test"
runOne "./$dir/mgl_test"
runOne "./$dir/atomic_utility_test"
runOne "./$dir/index_mgr_test"
runOne "./$dir/status_test"
runOne "./$dir/skiplist_test"
runOne "./$dir/varint_test"
runOne "./$dir/server_params_test"
runOne "./$dir/command_test"
runOne "./$dir/network_test"
runOne "./$dir/rocks_kvstore_test"
runOne "./$dir/script_test"
runOne "./$dir/worker_pool_test"
runOne "./$dir/utils_common_test"
