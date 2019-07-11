tclsh tests/test_helper.tcl --single rr_unit/type/string
tclsh tests/test_helper.tcl  --single  rr_unit/type/hash
#tclsh tests/test_helper.tcl  --single  rr_unit/type/hscan
tclsh tests/test_helper.tcl  --single  rr_unit/type/list-2
tclsh tests/test_helper.tcl  --single  rr_unit/type/list-3
#tclsh tests/test_helper.tcl  --single  rr_unit/type/list-common
tclsh tests/test_helper.tcl  --single  rr_unit/type/list
tclsh tests/test_helper.tcl  --single  rr_unit/type/set
tclsh tests/test_helper.tcl  --single  rr_unit/type/zset
tclsh tests/test_helper.tcl  --single  rr_unit/hyperloglog
tclsh tests/test_helper.tcl  --single rr_unit/expire
tclsh tests/test_helper.tcl --single rr_unit/bitops
tclsh tests/test_helper.tcl --single rr_unit/auth
tclsh tests/test_helper.tcl --single rr_unit/basic
tclsh tests/test_helper.tcl --single rr_unit/protocol  
tclsh tests/test_helper.tcl --single rr_unit/other

tclsh tests/test_helper.tcl --single rr_unit/quit
tclsh tests/test_helper.tcl --single rr_unit/sort
tclsh tests/test_helper.tcl --single rr_unit/bugs


valgrind=0
#tests=(aofrw bitfield dump geo introspection-2 keyspace lazyfree maxmemory multi other protocol quit scripting sort wait auth bitops expire hyperloglog introspection latency-monitor limits memefficiency obuf-limits printver pubsub scan slowlog)
tests=(bitfield dump keyspace other protocol quit sort auth bitops expire hyperloglog limits scan slowlog)
length=${#tests[@]}
length=`expr $length - 1`
for i in `seq 0 $length`
do
        if [ $valgrind -eq 1 ]; then
                tclsh tests/test_helper.tcl --valgrind --single cluster_test/${tests[$i]};
        else
                tclsh tests/test_helper.tcl --single cluster_test/${tests[$i]};
        fi
done

tests=(hash incr list-2 list-3 list set string zset)
length=${#tests[@]}
length=`expr $length - 1`
for i in `seq 0 $length`
do
        if [ $valgrind -eq 1 ]; then
                tclsh tests/test_helper.tcl --valgrind --single cluster_test/type/${tests[$i]};
        else
                tclsh tests/test_helper.tcl --single cluster_test/type/${tests[$i]};
        fi
done


