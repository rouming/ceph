#!/bin/bash

set -e

SERVERS=$1
SCRIPT=`realpath $0`
SCRIPTPATH=`dirname $SCRIPT`
BUILDPATH=$SCRIPTPATH/../build

if [ $# == 0 ]; then
	echo "Usage: <servers>"
	echo "e.g.: blueshark-[1-8]"
	exit 1
fi

parse_servers()
{
	args=("$@");
	for host in "${args[@]}"; do
		rep=`echo $host | perl -ne 'print "$1 $2" if /\[(\d+)-(\d+)\]/'`
		sub=`echo $host | perl -ne 'print "$1" if /(.*?)\[\d+-\d+\]/'`

		if [ "$rep" != "" ]; then
			for i in `seq $rep`; do
				printf "$sub$i\n"
			done
		else
			printf "$host\n"
		fi
	done
}

# Convert multi-line variable to an array
declare -a SERVERS_ARR
while IFS= read -r host; do
	SERVERS_ARR+=("$host")
done <<< `parse_servers "$@"`

sync()
{
	host=$1

	echo ">>>>>>>>>>>>> $host <<<<<<<<<<<<<<<<"
	ssh $host mkdir -p /var/lib/ceph
	ssh $host mkdir -p /var/lib/ceph/out
	ssh $host rm -f /var/lib/ceph/out/ceph*.asok

	scp /tmp/ceph.conf.new $host:/var/lib/ceph/ceph.conf

	rsync --info=progress2 -av  \
              --exclude 'dev/mon*'       \
		$BUILDPATH/systemd       \
		$BUILDPATH/../src/pybind \
		$BUILDPATH/lib           \
		$BUILDPATH/bin           \
		$BUILDPATH/dev           \
		$host:/var/lib/ceph/

	# Needed for fio server, which reads config from the default place
	ssh $host mkdir -p /etc/ceph
	ssh $host ln -sf /var/lib/ceph/ceph.conf /etc/ceph

	ssh $host ln -sf /var/lib/ceph/systemd/pech-osd@.service /usr/lib/systemd/system/pech-osd@.service
	ssh $host ln -sf /var/lib/ceph/systemd/ceph-osd@.service /usr/lib/systemd/system/ceph-osd@.service
	ssh $host ln -sf /var/lib/ceph/systemd/crimson-osd@.service /usr/lib/systemd/system/crimson-osd@.service
	ssh $host ln -sf /var/lib/ceph/systemd/fio.service /usr/lib/systemd/system/fio.service
	ssh $host systemctl daemon-reload
}

# Prepare new config
sed 's:/root/devel/ceph/build:/var/lib/ceph:' ceph.conf > /tmp/ceph.conf.new
sed -i 's:/root/devel/ceph:/var/lib/ceph:' /tmp/ceph.conf.new
# comment out admin socket
sed -i 's:admin[ _]\+socket:#admin socket:' /tmp/ceph.conf.new

# Iterate over an array
for host in "${SERVERS_ARR[@]}"; do
	sync $host &
done

wait
