#!/bin/bash

path=/users/gustavo/go/src/beexecutor

inputsLocation="/tmp/input"

workloads=("workloada" "workloadalatest" "workloadaprime" "workloadb" "workloadd")
logstratnames=("notlog" "trad" "beelog" "tradbatch")

logFolder="/media/disk1"

secondDisk="/media/disk2"
#secondDisk=""

# ---------------------------------------
# local config:
# ---------------------------------------
#path=/home/lzgustavo/go/src/beexecutor
#inputsLocation="/home/lzgustavo/Beelog-Exp/inputLogs/"
#workloads=("workloada")

#logFolder="/tmp/logs"
#secondDisk="/tmp/logs2"
#----------------------------------------

persistInterval=1000
beelogConcLevel=2

syncIO=true
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

	echo "[info] creating ${dir} dir..."
	mkdir -p ${dir} # no error if exists
	mkdir -p ${logFolder}/${i}

	echo "[info] running for ${i}..."
	# not empty
	if [[ ! -z "${secondDisk}" ]]; then
		echo "[info] 2 disks config"
		mkdir -p ${secondDisk}/${i}
		$path/beexecutor -input="${inputsLocation}/${i}.log" -logstrat=${2} -interval=${persistInterval} -conclevel=${beelogConcLevel} -sync=${syncIO} -latency=${latOut} -logfolder="${logFolder}/${i}/" -secdisk="${secondDisk}/${i}/" -output=${dir} -timeout=${timeout}
	else
		$path/beexecutor -input="${inputsLocation}/${i}.log" -logstrat=${2} -interval=${persistInterval} -conclevel=${beelogConcLevel} -sync=${syncIO} -latency=${latOut} -logfolder="${logFolder}/${i}/" -output=${dir} -timeout=${timeout}
	fi
	echo "[info] finished generating load ${i}..."

	if [[ ${deleteLogsOutput} -eq 1 ]]; then
		echo "[info] deleting log files..."
		find ${logFolder} -name "*.log" -delete
		find ${secondDisk} -name "*.log" -delete
	fi

	if [[ ${latOut} == "true" ]]; then
		echo "[info] moving latency file..."
		mv ${logFolder}/${i}/*.out ${1}/${i}/
	fi
	echo ""
done

echo "finished!"
