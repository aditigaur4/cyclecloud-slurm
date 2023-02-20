#!/bin/bash
set -x
set -e 
log=/tmp/prolog.log
#echo $SLURM_JOB_NODELIST >> /tmp/prolog.log
nodename=$(/bin/scontrol show hostnames $SLURM_JOB_NODELIST | head -n 1)
echo $nodename,$SLURM_JOB_ID >> $log
json=$(/opt/cycle/slurm/get_acct_info.sh $nodename 2>>$log)
ret=$(echo $json | jq '. | length')

echo "json: " $json "ret: " $ret >> $log
while [ $ret == 0 ];
do
        sleep 1
        json=$(/opt/cycle/slurm/get_acct_info.sh $nodename 2>>$log)
        ret=$(echo $json | jq '. | length')
done
sku_name=$( echo $json | jq .[0].vm_size -r)
region=$( echo $json | jq .[0].location -r)
spot=$( echo $json | jq .[0].spot -r)
tcpus=$( echo $json | jq .[0].cpus -r)
commentstr="sku=$sku_name,region=$region,spot=$spot,cpu=$tcpus"
echo $commentstr, $SLURM_JOB_ID >> $log
/bin/scontrol update job=$SLURM_JOB_ID, admincomment=$commentstr
