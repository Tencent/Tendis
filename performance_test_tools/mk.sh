storecount=8
diskcount=4
let mod=$storecount%$diskcount
if [ $mod != 0 ]
then
    echo storecount is wrong: $storecount
    exit -1
fi

for (( i=0; i<$diskcount; i++ ))
do
    if [ $i != 0 ]
    then
        let suffix=$i+1
        disk=cbs$suffix

        rm /mnt/$disk/tendisplus/home/* -rf
    else
        rm ./home/* -rf
    fi
done

let pernum=$storecount/$diskcount
for (( i=0; i<$diskcount; i++ ))
do
    for (( j=0; j<$pernum; j++ ))
    do
        let storeno=$pernum*$i+$j
        if [ $i != 0 ]
        then
            let suffix=$i+1
            disk=cbs$suffix

            mkdir /mnt/$disk/tendisplus/home/db/$storeno -p
            ln -s /mnt/$disk/tendisplus/home/db/$storeno ./home/db/$storeno

            mkdir /mnt/$disk/tendisplus/home/dump/$storeno -p
            ln -s /mnt/$disk/tendisplus/home/dump/$storeno ./home/dump/$storeno
        else
            mkdir ./home/db/$storeno -p
            mkdir ./home/dump/$storeno -p
        fi
    done
done
