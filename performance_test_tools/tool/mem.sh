source ./conf.sh

log="mem.log"
stat_dir=info
mkdir -p $stat_dir

inter_sec=20
iostat_sec=2
qps_sec=5
let sleep_set=$inter_sec-$iostat_sec-$qps_sec

while(true)
do
    echo `date +"%Y/%m/%d %H:%M:%S"` >> $log
    echo `ps axu |grep tendisplus|grep -v grep` >> $log
    free -m >> $log
    top -n 1 -b |head -10 >> $log
    echo `du -h ${script_dir}/home/db/ |tail -1` >> $log
    echo `du -h ${script_dir}/home/dump/ |tail -1` >> $log
    echo `iostat $iostat_sec 2|grep vd|tail -${disk_num} |awk '{print $1 " readKB:" $3 " writeKB:"$4}'` >> $log
    ts=`date +"%Y/%m/%d-%H:%M:" --date "2 min ago"`
    ./compaction.sh "*" $ts >> $log

    time_suffix=`date +"%Y-%m-%d--%H-%M"`
    ${bin_dir}/redis-cli -h $ip -p $port $cli_pw tendisstat > $stat_dir/stat.$time_suffix
    ${bin_dir}/redis-cli -h $ip -p $port $cli_pw info > $stat_dir/info.$time_suffix

    start=`${bin_dir}/redis-cli -h $ip -p $port $cli_pw tendisstat|grep processed |awk -F " |:|," '{print $11}'`
    sleep $qps_sec
    end=`${bin_dir}/redis-cli -h $ip -p $port $cli_pw tendisstat|grep processed |awk -F " |:|," '{print $11}'`
    let qps=($end-$start)/$qps_sec
    echo "qps:$qps" >> $log

    echo "" >> $log

    sleep $sleep_set
done
