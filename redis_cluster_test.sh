#!/bin/bash
tclsh tests/cluster/run.tcl --single 00-base.tcl
tclsh tests/cluster/run.tcl --single 01-faildet.tcl
tclsh tests/cluster/run.tcl --single 02-failover.tcl
tclsh tests/cluster/run.tcl --single 03-failover-loop.tcl
tclsh tests/cluster/run.tcl --single 04-resharding.tcl
tclsh tests/cluster/run.tcl --single 05-slave-selection.tcl
tclsh tests/cluster/run.tcl --single 06-slave-stop-cond.tcl
tclsh tests/cluster/run.tcl --single 07-replica-migration.tcl
tclsh tests/cluster/run.tcl --single 08-update-msg.tcl
tclsh tests/cluster/run.tcl --single 09-pubsub.tcl
tclsh tests/cluster/run.tcl --single 10-manual-failover.tcl
tclsh tests/cluster/run.tcl --single 11-manual-takeover.tcl
tclsh tests/cluster/run.tcl --single 12-replica-migration-2.tcl
tclsh tests/cluster/run.tcl --single 13-no-failover-option.tcl
tclsh tests/cluster/run.tcl --single 14-manual-failover-force.tcl
tclsh tests/cluster/run.tcl --single 15-arbiter-selection.tcl