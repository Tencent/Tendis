#!/bin/sh

sh ./unittest.sh >&unittest.log &

build/bin/repl_test >&repl_test.log &
build/bin/restore_test >&restore_test.log &

sh ./redistest.sh >&redistest.log 

cd src/tendisplus/integrate_test
./gotest.sh &
cd -

grep -En "\[err|\[exception" redistest.log
:<<!
grep PASSED repl_test.log
grep PASSED restore_test.log
grep -E 'Expected|FAILED' unittest.log
grep "compare " src/tendisplus/integrate_test/gotest.log
!
