logfile=$1
mergeinter=10

tmpfile=${logfile}.tmp
outfile=${logfile}.out

cat $logfile |grep -v flag|grep -v start|grep -v qtps|awk -F '[ \|]+' '{print $10}'|sed "s/K/000/g" > $tmpfile

rm $outfile
num=0
sum=0
while read line
do
    if [ $line == 0 ]
    then
        continue
    fi
    let sum+=$line
    let num+=1
    let mod=$num%$mergeinter
    if [ $mod == 0 ]
    then
        let aver=$sum/$mergeinter
        echo $aver >> $outfile
        sum=0
    fi
done < $tmpfile
