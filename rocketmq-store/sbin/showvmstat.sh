#!/bin/sh
while [ "1" == "1" ]
do
	date
	cat /proc/vmstat |egrep "nr_free_pages|nr_anon_pages|nr_file_pages|nr_dirty|nr_writeback|pgpgout|pgsteal_normal|pgscan_kswapd_normal|pgscan_direct_normal|kswapd_steal"
	echo
	sleep 1
done
