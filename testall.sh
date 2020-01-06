#!/bin/sh

sh ./unittest.sh >&unittest.log &

build/bin/repl_test >&repl_test.log &
build/bin/restore_test >&restore_test.log &

sh ./redistest.sh >&redistest.log 

grep -En "\[err|\[exception" redistest.log

cd src/tendisplus/integrate_test
./gotest.sh &
cd -

