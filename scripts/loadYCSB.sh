#!/bin/bash

#path=/home/lzgustavo/go/src/go-ycsb
path=/users/gustavo/go/src/go-ycsb

workloads=("workloada" "workloadb" "workloadc" "workloadd" "workloaddprime")
numDiffKeys=1000000 # 1kk
threadCount=1

numOps=10000000 # 10kk

if [[ $# -ne 1 ]]; then
	echo "usage: $0 'rootFolder'"
	exit 1
fi

#echo "compiling go-ycsb..."
#make -C $path

echo "running..."
for i in ${workloads[*]}; do
	$path/bin/go-ycsb run loadgenkv -P $path/workloads/${i} -p threadcount=${threadCount} -p recordcount=${numDiffKeys} -p operationcount=${numOps} -p loadgenkv.outfilename="${1}/${i}.log"
	echo "finished generating load ${i}..."; echo ""
done

echo "finished!"
