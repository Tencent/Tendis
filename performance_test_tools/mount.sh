mkfs.ext4 /dev/vdb
mkdir /mnt/cbs
mount /dev/vdb /mnt/cbs/
yum install sysstat
yum install perf
yum install lrzsz
