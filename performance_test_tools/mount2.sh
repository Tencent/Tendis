mkfs.ext4 /dev/vdb
mkfs.ext4 /dev/vdc
mkfs.ext4 /dev/vdd
mkfs.ext4 /dev/vde
mkdir /mnt/cbs
mkdir /mnt/cbs2
mkdir /mnt/cbs3
mkdir /mnt/cbs4
mount /dev/vdb /mnt/cbs/
mount /dev/vdc /mnt/cbs2/
mount /dev/vdd /mnt/cbs3/
mount /dev/vde /mnt/cbs4/
yum install sysstat
yum install perf
yum install lrzsz
