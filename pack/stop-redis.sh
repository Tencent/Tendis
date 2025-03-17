#!/bin/sh
#start a redis server
#by tencent dba @ 202208

#switch to current dir
CDIR=`dirname $0`

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
	usage
	exit
fi

shift

DISKROOT="$REDIS_DATA_DIR"
if [ -z $DISKROOT ]
then
	if [ -d "/data1/redis" ]
	then
		DISKROOT="/data1"
	elif [ -d "/data/redis" ]
	then
		DISKROOT="/data"
	else
		echo "cannot find data directory"
		exit -1
	fi
fi

rootdir="${DISKROOT}/redis/$PORT"
datadir="${rootdir}/data"
confpath="${rootdir}/redis.conf"

address=$(grep 'bind' $confpath|awk '{print $2}'|xargs)

opt=""
version=$($CDIR/redis-cli --version|awk '{print $2}')
if [[ $version > "6.0.0" ]]
then
	opt="--no-auth-warning"
fi

echo "$CDIR/redis-cli $opt -h $address -p $PORT $PASS_OPT $PASS shutdown"
$CDIR/redis-cli $opt -h $address -p $PORT $PASS_OPT $PASS shutdown
