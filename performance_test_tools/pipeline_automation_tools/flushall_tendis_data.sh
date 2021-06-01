#!/usr/bin/env sh
redisDomain=`grep "$POD_NAME" /data/redis/30000/redis.conf|awk '{print $2}'`

/usr/local/redis/bin/redis-cli -a $REDIS_PASSWORD -p $TENDIS_PORT  -h $redisDomain cluster nodes|grep "master" |awk '{print $2}'|awk -F @ '{print $1}'|awk -F : '{print $1,$2}'|while read redisIP redisPort
do
    echo "==>$redisIP#$redisPort"
    /usr/local/redis/bin/redis-cli -a $REDIS_PASSWORD -p $redisPort  -h $redisIP  flushall  2>/dev/null
done
