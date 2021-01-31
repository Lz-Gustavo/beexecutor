#!/bin/bash

path=/Users/lzgustavo/go/src/go-ycsb
#path=/users/gustavo/go/src/go-ycsb

etcdHostname="127.0.0.1"
workload="workloada"
threadCounts=(1 4 7 10 13 16 19)

numDiffKeys=1000000 # 1kk
targetThr=10000
#numOps=10000000 # 10kk
numOps=600000

if [[ $# -ne 1 ]]; then
	echo "usage: $0 'rootFolder'"
	exit 1
fi

#echo "compiling go-ycsb..."
#make -C $path

echo "running..."
for t in ${threadCounts[*]}; do
	$path/bin/go-ycsb run etcd -P $path/workloads/${workload} -p threadcount=${t} -p recordcount=${numDiffKeys} -p operationcount=${numOps} -p target=${targetThr} -p etcd.hostname=${etcdHostname} -p etcd.latfilename="${1}/${workload}/${t}c-lat.out"
	echo "finished ${t} client threads..."; echo ""
done

echo "finished!"
