#!/bin/sh
#start a redis server
#by tencent dba @ 202208

#switch to current dir
CDIR=`dirname $0`
cd $CDIR

function usage () {
	echo "usage:"
	echo "$0 3689"
	echo "$0 3689 PASSWORD"
	echo "$0 3689 + some redis arg like: $0 3689 --slaveof 1.1.1.1 3679"
}

PORT=$1
PASS_OPT=""
PASS=""
if [ $# -eq 2 ]
then
    PASS_OPT=" -a "
    PASS="$2"
fi

if [ ! -n "$PORT"  ];then
	echo "PORT not set, exit"
	usage;
	exit;
fi

shift

$CDIR/redis-cli -p $PORT $PASS_OPT $PASS shutdown
