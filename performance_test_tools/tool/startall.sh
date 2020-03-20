source ./conf.sh

stopshell(){
    ps axu |grep $1 |grep $user|awk '{print $2}'|xargs kill -9
}

stopshell mem.sh
nohup ./mem.sh &

stopshell qps.sh
nohup ./qps.sh &

stopshell dump_clear.sh
nohup ./dump_clear.sh &
