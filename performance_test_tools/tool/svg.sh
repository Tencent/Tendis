if [ $# != 1 ]
then
    echo "./svg.sh pid"
    exit -1
fi
pid=$1
time=`date "+%Y%m%d-%H%M%S"`

#perf record -F 99 -p $pid -m 4 -g -a -- sleep 5
perf record -F 99 -p $pid -m 4 -g -a sleep 5
cp perf.data perf.data.$time
perf script > out.perf 
./FlameGraph-master/stackcollapse-perf.pl out.perf >out.folded
./FlameGraph-master/flamegraph.pl out.folded > pmCount.svg
file=$time.svg
mv pmCount.svg $file
sz $file
