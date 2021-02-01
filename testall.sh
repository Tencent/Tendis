#!/bin/bash

sh ./unittest.sh >&unittest.log &

build/bin/repl_test >&repl_test.log &
build/bin/restore_test >&restore_test.log &
build/bin/cluster_test >&cluster_test.log &

sh ./redistest.sh &

cd src/tendisplus/integrate_test
./gotest.sh &
cd -

grep -En "\[err|\[exception|49merr|49mexception" redistest.log
:<<!
grep PASSED repl_test.log
grep PASSED restore_test.log
grep PASSED cluster_test.log
grep -E 'Expected|FAILED' unittest.log
grep "go passed" src/tendisplus/integrate_test/gotest.log
!
