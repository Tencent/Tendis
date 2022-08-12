#!/bin/sh
#start a redis server
#by tencent dba @ 202208

function usage () {
	echo "usage:"
	echo "$0 3689" 
	echo "$0 3689 + some redis arg like: $0 3689 --slaveof 1.1.1.1 3679" 
}

PORT=$1

if [ ! -n "$PORT"  ];then
	echo "PORT not set, exit"
	usage;
	exit;
fi

shift

#switch to current dir
CDIR=`dirname $0`
cd $CDIR

#check if there is /data1 exist mounted, if exists,use it.
EX_DATA1=`df -h|grep -E "/data1$" |wc -l`

#default dir
DISKROOT="/data"

if [ $EX_DATA1 -gt 0 ]
then
    DISKROOT="/data1"
else
    DISKROOT="/data"
fi


rootdir="${DISKROOT}/redis/$PORT"
datadir="${rootdir}/data"
confpath="${rootdir}/redis.conf"
dbDir="${datadir}/db"
dumpDir="${datadir}/dump"
logDir="${datadir}/log"

if [ ! -d "$rootdir" ];then
	echo "dir $rootdir not exists"
	usage;
	exit;
fi

if [ ! -f "$confpath" ];then
	echo "file $confpath not exists"
	usage;
	exit;
fi

if [ ! -d "$dbDir" ];then
	mkdir -p $dbDir
fi

if [ ! -d "$dumpDir" ];then
	mkdir -p $dumpDir
fi

if [ ! -d "$logDir" ];then
	mkdir -p $logDir
fi

export LD_LIBRARY_PATH=LD_LIBRARY_PATH:./deps 
$CDIR/tendisplus $confpath
