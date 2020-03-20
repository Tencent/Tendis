source ./conf.sh

log="qps.log"

while(true)
do
    num=`ps axu |grep stat |grep host|grep $user|grep $ip |grep -v grep |wc -l`
    if [ $num -lt 1 ]
    then
        echo `date +"%Y/%m/%d %H:%M:%S"` num:$num start stat >> $log
        nohup ./stat -host $ip -port $port $stat_pw >> $log &
    fi
    echo `date +"%Y/%m/%d %H:%M:%S"` time flag  >> $log
    sleep 60
done
