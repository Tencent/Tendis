source ./conf.sh

time=$2
dbid=$1
dir=$script_dir/$data_dir
echo $time
#file=LOG.old.*
file=LOG

flush_bytes=`grep "flush table" $dir/db/$dbid/$file|grep $time|grep "bytes OK"|awk 'BEGIN{sum=0}; {sum+=$11}; END {print sum}'`
let flush_mb=$flush_bytes/1024/1024
echo flush mb:$flush_mb

compation_bytes=`grep compaction_started $dir/db/$dbid/$file|grep $time |awk -F "input_data_size" '{print $2}' |awk -F " |}" 'BEGIN{sum=0};{sum=sum+$2}; END{print sum}'`
let compation_mb=$compation_bytes/1024/1024
echo compaction mb:$compation_mb
