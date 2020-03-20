source ./conf.sh

ps axu |grep redis-benchmark |grep $user|awk '{print $2}'|xargs kill -9

clientnum=100
benchnum=2
port=5555
keynum=900000000
for((i=0;i<$benchnum;i++))
do
    #nohup ~/git/tendisplus/bin/redis-benchmark -h 127.0.0.1 -p $port -c $clientnum -n 100000000 -r 8 -i -f $i -t set -a tendis+test & 
    nohup ./redis-benchmark -h $benchip -p $benchport -c $clientnum -n $keynum -r 1000000000 -d 128 -t set $bench_pw & 
done
