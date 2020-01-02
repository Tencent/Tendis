rm ./master1/  ./master2/ ./back_test/ -rf
rm ./m1_*/ ./m2_*/ ./s1_*/ ./s2_*/ ./m_*/ ./s_* -rf 
user=takenliu
ps axu |grep tendisplus|grep integrate_test| grep $user|awk '{print $2}'|xargs kill -9
while true
do
    a=`ps axu |grep tendisplus|grep $user|wc -l`
    if [ "$a" == "1" ] || [ "$a" == "0" ]
    then
        break
    else
        sleep 1
    fi
done
