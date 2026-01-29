#!/bin/bash
# Local NFS Setup Script - Supports CentOS/RHEL/TencentOS/Ubuntu/Debian/Fedora
set -e

SHARE_PATH="/shared/rocksdb"
MOUNT_PATH="/mnt/rocksdb"
LOCAL_DB_PATH="./home/db"
CLEAN_MODE=false

# Detect OS and set variables
detect_os() {
    [ -f /etc/os-release ] && . /etc/os-release || OS_ID="centos"
    OS_ID="${ID:-$OS_ID}"
    
    case "$OS_ID" in
        ubuntu|debian)
            PKG_MGR="apt-get"; NFS_PKG="nfs-kernel-server nfs-common"; NFS_SVC="nfs-kernel-server" ;;
        centos|rhel|rocky|almalinux|ol|tencentos|tlinux|opencloudos|anolis)
            PKG_MGR="yum"; NFS_PKG="nfs-utils"; NFS_SVC="nfs-server" ;;
        fedora)
            PKG_MGR="dnf"; NFS_PKG="nfs-utils"; NFS_SVC="nfs-server" ;;
        *) echo "Unsupported OS: $OS_ID"; exit 1 ;;
    esac
    echo "OS: $OS_ID"
}

# Install NFS
install_nfs() {
    echo "=== Installing NFS ==="
    case "$PKG_MGR" in
        apt-get) sudo apt-get update -qq && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y $NFS_PKG ;;
        *) sudo $PKG_MGR install -y $NFS_PKG ;;
    esac
}

# Start NFS services
start_nfs() {
    echo "=== Starting NFS ==="
    sudo systemctl start rpcbind $NFS_SVC 2>/dev/null || true
    sudo systemctl enable rpcbind $NFS_SVC 2>/dev/null || true
    sleep 2
}

# Clean data
clean_data() {
    echo "=== Cleaning Data ==="
    echo "Share path: $SHARE_PATH"
    echo "Local DB path: $LOCAL_DB_PATH"
    rm -rf "$SHARE_PATH"/* 2>/dev/null || true
    rm -rf "$LOCAL_DB_PATH"/* 2>/dev/null || true
    echo "Done"
}

# Usage
usage() {
    cat <<EOF
Usage: $0 [-s share_path] [-m mount_path] [-l local_db_path] [-c] [-h]
  -s PATH  Share directory (default: /shared/rocksdb)
  -m PATH  Mount point (default: /mnt/rocksdb)
  -l PATH  Local DB path for clean (default: ./home/db)
  -c       Clean mode only
  -h       Help
EOF
    exit 0
}

# Parse args
while getopts "s:m:l:ch" opt; do
    case $opt in
        s) SHARE_PATH="$OPTARG" ;;
        m) MOUNT_PATH="$OPTARG" ;;
        l) LOCAL_DB_PATH="$OPTARG" ;;
        c) CLEAN_MODE=true ;;
        h) usage ;;
        *) usage ;;
    esac
done

# Clean mode
if [ "$CLEAN_MODE" = true ]; then
    clean_data
    exit 0
fi

echo "=== NFS Setup: $SHARE_PATH -> $MOUNT_PATH ==="
detect_os
install_nfs

# Create share dir
echo "=== Creating Directories ==="
sudo mkdir -p "$SHARE_PATH" "$MOUNT_PATH"
sudo chown -R "$(id -un):$(id -gn)" "$SHARE_PATH"
chmod 755 "$SHARE_PATH"

# Configure exports
echo "=== Configuring Exports ==="
EXPORT_LINE="$SHARE_PATH *(rw,sync,no_root_squash,no_subtree_check,insecure)"
grep -q "^$SHARE_PATH " /etc/exports 2>/dev/null && \
    sudo sed -i "s|^$SHARE_PATH .*|$EXPORT_LINE|" /etc/exports || \
    echo "$EXPORT_LINE" | sudo tee -a /etc/exports

start_nfs
sudo exportfs -arv
showmount -e localhost

# Mount
echo "=== Mounting ==="
mountpoint -q "$MOUNT_PATH" 2>/dev/null && sudo umount "$MOUNT_PATH"
sudo mount -t nfs localhost:"$SHARE_PATH" "$MOUNT_PATH"

# Verify
echo "=== Verifying ==="
echo 'test' > "$SHARE_PATH/test.txt" && cat "$MOUNT_PATH/test.txt" && rm "$MOUNT_PATH/test.txt"
echo "=== Complete: $SHARE_PATH mounted at $MOUNT_PATH ==="
