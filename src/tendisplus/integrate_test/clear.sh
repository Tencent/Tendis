rm ./master1/  ./master2/ ./back_test/ -rf
rm ./m1_*/ ./m2_*/ ./s1_*/ ./s2_*/ ./m_*/ ./s_* -rf 
rm ./m*_*/ -rf
user=takenliu
ps axu |grep tendisplus|grep integrate_test| grep $user|awk '{print $2}'|xargs kill -9
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
