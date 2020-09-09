#!/bin/bash

#path=/home/lzgustavo/go/src/beexecutor
path=/users/gustavo/go/src/beexecutor

inputsLocation="/tmp/input"
workloads=("workloada" "workloadb" "workloadc" "workloadd" "workloaddprime")
logstratnames=("notlog" "trad" "beelog")

logFolder="/tmp/logs"
beelogInterval=1000
beelogConcLevel=2

syncIO=false
latOut=true
timeout=-1

# 1: true, 0: false
deleteLogsOutput=1

if [[ $# -ne 2 ]]; then
	echo "usage: $0 'experimentFolder' 'logstrat (0: notlog, 1: tradlog, 2: beelog)'"
	exit 1
fi

if [[ ${2} -lt 0 ]] || [[ ${2} -gt 2 ]]; then
	echo "unsupported log strategy ${2} provided"
	exit 1
fi

if [[ ${2} -eq 2 ]]; then
	# interval logfolder
	logFolder="${logFolder}/int-${beelogInterval}"
fi

for i in ${workloads[*]}; do
	# root/workload/logstrat
	dir="${1}/${i}/${logstratnames[${2}]}/"

	echo "creating ${dir} dir..."
	mkdir -p ${dir} # no error if exists
	mkdir -p ${logFolder}/${i}

	echo "running for ${i}..."
	$path/beexecutor -input="${inputsLocation}/${i}.log" -logstrat=${2} -interval=${beelogInterval} -conclevel=${beelogConcLevel} -sync=${syncIO} -latency=${latOut} -logfolder="${logFolder}/${i}/" -output=${dir} -timeout=${timeout}
	echo "finished generating load ${i}..."; echo ""

	if [[ ${deleteLogsOutput} -eq 1 ]]; then
		echo "deleting log files..."
		find ${logFolder} -name "*.log" -delete
	fi
done

echo "finished!"
