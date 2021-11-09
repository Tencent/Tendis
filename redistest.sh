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
    errcnt1=$(grep main $logfile|wc -l)
    errcnt2=$(grep Jumping $logfile|wc -l)
    let errcnt=errcnt+errcnt1+errcnt2
    if [ $errcnt -ne 0 ]; then
        grep -E "\[err|\[exception|49merr|49mexception" $logfile
        exit $errcnt
    fi
}

runOne "tclsh tests/test_helper.tcl --single rr_unit/type/hash"
runOne "tclsh tests/test_helper.tcl --single rr_unit/type/hscan"
runOne "tclsh tests/test_helper.tcl --single rr_unit/type/list-2"
runOne "tclsh tests/test_helper.tcl --single rr_unit/type/list-3"
# runOne "tclsh tests/test_helper.tcl --single rr_unit/type/list-common"
runOne "tclsh tests/test_helper.tcl --single rr_unit/type/list"
runOne "tclsh tests/test_helper.tcl --single rr_unit/type/set"
runOne "tclsh tests/test_helper.tcl --single rr_unit/type/string"
runOne "tclsh tests/test_helper.tcl --single rr_unit/type/zset"

runOne "tclsh tests/test_helper.tcl --single rr_unit/auth"
runOne "tclsh tests/test_helper.tcl --single rr_unit/basic"
runOne "tclsh tests/test_helper.tcl --single rr_unit/bitops"
runOne "tclsh tests/test_helper.tcl --single rr_unit/expire"
runOne "tclsh tests/test_helper.tcl --single rr_unit/hyperloglog"
runOne "tclsh tests/test_helper.tcl --single rr_unit/other"
runOne "tclsh tests/test_helper.tcl --single rr_unit/protocol"
runOne "tclsh tests/test_helper.tcl --single rr_unit/quit"
runOne "tclsh tests/test_helper.tcl --single rr_unit/scan"
runOne "tclsh tests/test_helper.tcl --single rr_unit/scripting"
runOne "tclsh tests/test_helper.tcl --single rr_unit/sort"

runOne "tclsh tests/test_helper.tcl --single tendis_ssd_test/bitfield"
runOne "tclsh tests/test_helper.tcl --single tendis_ssd_test/cas"
runOne "tclsh tests/test_helper.tcl --single tendis_ssd_test/hmcas"
runOne "tclsh tests/test_helper.tcl --single tendis_ssd_test/increx"
runOne "tclsh tests/test_helper.tcl --single tendis_ssd_test/setnxex"
runOne "tclsh tests/test_helper.tcl --single tendis_ssd_test/zscanbyscore"

valgrind=0
tests=(bitfield dump keyspace other protocol quit sort auth bitops expire hyperloglog limits scan slowlog badkey)
for i in ${tests[@]}
do
    if [ $valgrind -eq 1 ]; then
        runOne "tclsh tests/test_helper.tcl --valgrind --single cluster_test/$i"
    else
        runOne "tclsh tests/test_helper.tcl --single cluster_test/$i"
    fi
done

tests=(hash incr list-2 list-3 list set string zset)
for i in ${tests[@]}
do
    if [ $valgrind -eq 1 ]; then
        runOne "tclsh tests/test_helper.tcl --valgrind --single cluster_test/type/$i"
    else
        runOne "tclsh tests/test_helper.tcl --single cluster_test/type/$i"
    fi
done
