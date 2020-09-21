#!/bin/bash

#path=/home/lzgustavo/go/src/beexecutor
path=/users/gustavo/go/src/beexecutor

inputsLocation="/tmp/input"

workloads=("workloada" "workloadb" "workloadc" "workloadd" "workloaddprime")
#workloads=("workloada")
logstratnames=("notlog" "trad" "beelog" "tradbatch")

logFolder="/tmp/logs"
secondDisk="/tmp/logs2"
#secondDisk=""

persistInterval=100
beelogConcLevel=2

syncIO=false
latOut=true
timeout=10

# 1: true, 0: false
deleteLogsOutput=1

if [[ $# -ne 2 ]]; then
	echo "usage: $0 'experimentFolder' 'logstrat (0: notlog, 1: tradlog, 2: beelog, 3: tradbatch)'"
	exit 1
fi

if [[ ${2} -lt 0 ]] || [[ ${2} -gt 3 ]]; then
	echo "unsupported log strategy ${2} provided"
	exit 1
fi

# if [[ ${2} -eq 2 ]]; then
# 	# interval logfolder
# 	logFolder="${logFolder}/int-${persistInterval}"
# fi

for i in ${workloads[*]}; do
	# root/workload/logstrat
	dir="${1}/${i}/${logstratnames[${2}]}/"

	echo "creating ${dir} dir..."
	mkdir -p ${dir} # no error if exists
	mkdir -p ${logFolder}/${i}

	echo "running for ${i}..."
	# not empty
	if [[ ! -z "${secondDisk}" ]]; then
		echo "info: 2 disks config"
		mkdir -p ${secondDisk}/${i}
		$path/beexecutor -input="${inputsLocation}/${i}.log" -logstrat=${2} -interval=${persistInterval} -conclevel=${beelogConcLevel} -sync=${syncIO} -latency=${latOut} -logfolder="${logFolder}/${i}/" -secdisk="${secondDisk}/${i}/" -output=${dir} -timeout=${timeout}
	else
		$path/beexecutor -input="${inputsLocation}/${i}.log" -logstrat=${2} -interval=${persistInterval} -conclevel=${beelogConcLevel} -sync=${syncIO} -latency=${latOut} -logfolder="${logFolder}/${i}/" -output=${dir} -timeout=${timeout}
	fi
	echo "finished generating load ${i}..."; echo ""

	if [[ ${deleteLogsOutput} -eq 1 ]]; then
		echo "deleting log files..."
		find ${logFolder} -name "*.log" -delete
		find ${secondDisk} -name "*.log" -delete
	fi
done

echo "finished!"
