#!/bin/bash
logfile=./redistest.log
rm $logfile

function runOne() {
    tmplog=./redistest_tmp.log
    rm $tmplog
    cmd=$1
    $cmd >> $tmplog 2>&1
    cat $tmplog
    cat $tmplog >> $logfile

    errcnt=`grep -E "\[err|\[exception|49merr|49mexception" $logfile|wc -l`
    if [ $errcnt -ne 0 ]; then
        grep -E "\[err|\[exception|49merr|49mexception" $logfile
        exit $errcnt
    fi
}

runOne "tclsh tests/test_helper.tcl --single rr_unit/type/string"
runOne "tclsh tests/test_helper.tcl  --single  rr_unit/type/hash"
#tclsh tests/test_helper.tcl  --single  rr_unit/type/hscan
runOne "tclsh tests/test_helper.tcl  --single  rr_unit/scan"
runOne "tclsh tests/test_helper.tcl  --single  rr_unit/type/list-2"
runOne "tclsh tests/test_helper.tcl  --single  rr_unit/type/list-3"
#tclsh tests/test_helper.tcl  --single  rr_unit/type/list-common
runOne "tclsh tests/test_helper.tcl  --single  rr_unit/type/list"
runOne "tclsh tests/test_helper.tcl  --single  rr_unit/type/set"
runOne "tclsh tests/test_helper.tcl  --single  rr_unit/type/zset"
runOne "tclsh tests/test_helper.tcl  --single  rr_unit/hyperloglog"
runOne "tclsh tests/test_helper.tcl  --single rr_unit/expire"
runOne "tclsh tests/test_helper.tcl --single rr_unit/bitops"
runOne "tclsh tests/test_helper.tcl --single rr_unit/auth"
runOne "tclsh tests/test_helper.tcl --single rr_unit/basic"
runOne "tclsh tests/test_helper.tcl --single rr_unit/protocol"
runOne "tclsh tests/test_helper.tcl --single rr_unit/other"

runOne "tclsh tests/test_helper.tcl --single rr_unit/quit"
runOne "tclsh tests/test_helper.tcl --single rr_unit/sort"
runOne "tclsh tests/test_helper.tcl --single rr_unit/bugs"
runOne "tclsh tests/test_helper.tcl --single rr_unit/scripting"

runOne "tclsh tests/test_helper.tcl --single tendis_ssd_test/zscanbyscore"
runOne "tclsh tests/test_helper.tcl --single tendis_ssd_test/hmcas"
runOne "tclsh tests/test_helper.tcl --single tendis_ssd_test/cas"
runOne "tclsh tests/test_helper.tcl --single tendis_ssd_test/setnxex"
runOne "tclsh tests/test_helper.tcl --single tendis_ssd_test/bitfield"
runOne "tclsh tests/test_helper.tcl --single tendis_ssd_test/increx"

runOne "tclsh tests/cluster/run.tcl --single 08"
runOne "tclsh tests/cluster/run.tcl --single 10"

valgrind=0
#tests=(aofrw bitfield dump geo introspection-2 keyspace lazyfree maxmemory multi other protocol quit scripting sort wait auth bitops expire hyperloglog introspection latency-monitor limits memefficiency obuf-limits printver pubsub scan slowlog)
tests=(bitfield dump keyspace other protocol quit sort auth bitops expire hyperloglog limits scan slowlog)
length=${#tests[@]}
length=`expr $length - 1`
for i in `seq 0 $length`
do
        if [ $valgrind -eq 1 ]; then
                runOne "tclsh tests/test_helper.tcl --valgrind --single cluster_test/${tests[$i]}"
        else
                runOne "tclsh tests/test_helper.tcl --single cluster_test/${tests[$i]}"
        fi
done

tests=(hash incr list-2 list-3 list set string zset)
length=${#tests[@]}
length=`expr $length - 1`
for i in `seq 0 $length`
do
        if [ $valgrind -eq 1 ]; then
                runOne "tclsh tests/test_helper.tcl --valgrind --single cluster_test/type/${tests[$i]}"
        else
                runOne "tclsh tests/test_helper.tcl --single cluster_test/type/${tests[$i]}"
        fi
done



