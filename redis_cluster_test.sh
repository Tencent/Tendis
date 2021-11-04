#!/bin/bash
logfile=./redisclustertest.log
rm $logfile

function runOne() {
    tmplog=./redisclustertest_tmp.log
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

    ps axu | grep tendisplus | grep $USER | awk '{print $2}' | xargs kill -9 | tee /dev/null
}

runOne "tclsh tests/cluster/run.tcl --single 00-base.tcl"
runOne "tclsh tests/cluster/run.tcl --single 01-faildet.tcl"
runOne "tclsh tests/cluster/run.tcl --single 02-failover.tcl"
runOne "tclsh tests/cluster/run.tcl --single 03-failover-loop.tcl"
runOne "tclsh tests/cluster/run.tcl --single 04-resharding.tcl"
runOne "tclsh tests/cluster/run.tcl --single 05-slave-selection.tcl"
runOne "tclsh tests/cluster/run.tcl --single 05.1-slave-selection.tcl"
runOne "tclsh tests/cluster/run.tcl --single 06-slave-stop-cond.tcl"
runOne "tclsh tests/cluster/run.tcl --single 07-replica-migration.tcl"
runOne "tclsh tests/cluster/run.tcl --single 08-update-msg.tcl"
# not implement pub/sub
# runOne "tclsh tests/cluster/run.tcl --single 09-pubsub.tcl"
runOne "tclsh tests/cluster/run.tcl --single 10-manual-failover.tcl"
runOne "tclsh tests/cluster/run.tcl --single 11-manual-takeover.tcl"
# not implement cluster rebalance
# runOne "tclsh tests/cluster/run.tcl --single 12-replica-migration-2.tcl"
# runOne "tclsh tests/cluster/run.tcl --single 12.1-replica-migration-3.tcl"
runOne "tclsh tests/cluster/run.tcl --single 13-no-failover-option.tcl"
runOne "tclsh tests/cluster/run.tcl --single 14-consistency-check.tcl"
runOne "tclsh tests/cluster/run.tcl --single 15-cluster-slots.tcl"
runOne "tclsh tests/cluster/run.tcl --single 16-transactions-on-replica.tcl"
# Tendis doesn't have the same 'swapdb' as Redis
# runOne "tclsh tests/cluster/run.tcl --single 17-diskless-load-swapdb.tcl"
# Tendis info output format not the same with recent version Redis
# runOne "tclsh tests/cluster/run.tcl --single 18-info.tcl"
runOne "tclsh tests/cluster/run.tcl --single 19-cluster-nodes-slots.tcl"
# Tendis data migrate cmds are different from Redis
# runOne "tclsh tests/cluster/run.tcl --single 20-half-migrated-slot.tcl"
# runOne "tclsh tests/cluster/run.tcl --single 21-many-slot-migration.tcl"

# Add by Tendis
# same with above tests
# runOne "tclsh tests/cluster/run.tcl --single 60-manual-failover-force.tcl"
runOne "tclsh tests/cluster/run.tcl --single 61-arbiter-selection.tcl"
