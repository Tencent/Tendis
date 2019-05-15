#!/bin/sh

sh ./unittest.sh >&unittest.log &

sh ./redistest.sh >&redistest.log 

grep "\[err" redistest.log 


