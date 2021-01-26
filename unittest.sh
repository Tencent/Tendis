#!/bin/bash

logfile=./unittest.log
rm $logfile

function runOne() {
    tmplog=./unittest_tmp.log
    rm $tmplog
    cmd="$1 --gtest_throw_on_failure"
    $cmd >> $tmplog 2>&1
    cat $tmplog
    cat $tmplog >> $logfile

    errcnt=`grep -E "Expected|FAILED" $logfile|wc -l`
    if [ $errcnt -ne 0 ]; then
        grep -E "Expected|FAILED" $logfile
        exit $errcnt
    fi
    #passcnt=`grep -E "\[  PASSED  \]" $tmplog|wc -l`
    #if [ $passcnt -lt 1 ]; then
    #    exit 1
    #fi
}

dir=build/bin

runOne "./$dir/record_test"
#./logging_unittest
#./utilities_unittest
#./db_sanity_test
runOne "./$dir/lock_test"
runOne "./$dir/mgl_test"
runOne "./$dir/signalhandler_unittest"
runOne "./$dir/symbolize_unittest"
runOne "./$dir/atomic_utility_test"
runOne "./$dir/index_mgr_test"
runOne "./$dir/stacktrace_unittest"
runOne "./$dir/status_test"
runOne "./$dir/skiplist_test"
runOne "./$dir/varint_test"
runOne "./$dir/server_params_test"
runOne "./$dir/command_test"
runOne "./$dir/demangle_unittest"
runOne "./$dir/stl_logging_unittest"
runOne "./$dir/network_test"
runOne "./$dir/rocks_kvstore_test"
runOne "./$dir/script_test"
