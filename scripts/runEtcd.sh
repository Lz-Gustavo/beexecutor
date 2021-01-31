#!/bin/bash

path=/users/gustavo/go/src/beexecutor/etcd

inputsLocation="/tmp/input"
workloads=("workloada" "workloadalatest" "workloadaprime" "workloadb" "workloadd")
etcdHostname="127.0.0.1"

# ---------------------------------------
# local config:
# ---------------------------------------
#path=/Users/lzgustavo/go/src/beexecutor/etcd
#inputsLocation="/Users/lzgustavo/Exp/inputLogs/"
#workloads=("workloada")
#----------------------------------------

timeout=2

# 1: true, 0: false
deleteLogsOutput=1

if [[ $# -ne 1 ]]; then
	echo "usage: $0 'experimentFolder'"
	exit 1
fi

for i in ${workloads[*]}; do
	# root/workload/
	dir="${1}/${i}/"

	echo "[info] creating ${dir} dir..."
	mkdir -p ${dir} # no error if exists

	echo "[info] running for ${i}..."
	$path/etcd -input="${inputsLocation}/${i}.log" -etcd=${etcdHostname} -latencyout=${dir} -timeout=${timeout}
	echo "[info] finished generating load ${i}..."
	echo ""
done

echo "finished!"
