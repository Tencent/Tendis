rm ./master1/  ./master2/ ./back_test/ -rf
rm ./m1_*/ ./m2_*/ ./s1_*/ ./s2_*/ ./m_*/ ./s_* -rf 
rm ./m*_*/ -rf
rm ./src_*/ ./dst_*/ -rf
rm -f tendisplus*.log
rm -f valgrindTendis*.log
rm predixy_* -rf

user=$USER
ps axu |grep tendisplus|grep integrate_test| grep $user|awk '{print $2}'|xargs kill -9
ps axu |grep predixy   |grep integrate_test| grep $user|awk '{print $2}'|xargs kill -9
while true
do
    a=`ps axu |grep tendisplus|grep integrate_test|grep $user|grep -v "grep" |wc -l`
    if [ "$a" == "0" ]
    then
        break
    else
        sleep 1
    fi
done
