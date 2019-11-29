#!/bin/bash
export GOPATH="/data/zhaohao/6.824"
export PATH="$PATH"
rm res -rf
mkdir res
go test -c -o kvraft.test

test_list=(
	TestBasic3A 
	TestConcurrent3A 
	TestUnreliable3A 
	TestUnreliableOneKey3A 
	TestOnePartition3A 
	TestManyPartitionsOneClient3A 
	TestManyPartitionsManyClients3A 
	TestPersistOneClient3A 
	TestPersistConcurrent3A
	TestPersistConcurrentUnreliable3A
	TestPersistPartition3A
	TestPersistPartitionUnreliable3A
	TestPersistPartitionUnreliableLinearizable3A
	TestSnapshotRPC3B
	TestSnapshotSize3B
	TestSnapshotRecover3B 
	TestSnapshotRecoverManyClients3B
	TestSnapshotUnreliable3B
	TestSnapshotUnreliableRecover3B
	TestSnapshotUnreliableRecoverConcurrentPartition3B
	TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B
	)
for i in `seq 100`;do
{ 
	for j in ${test_list[@]}; do
	{
		for z in `seq 5`;do
		{
			./kvraft.test -test.v -test.run $j &> ./res/$z
			if [ $? -ne 0 ]
			then
				 echo "run failed. $j $z"
				 cp ./res/$z ./res/error_$z
				 exit 1
			 fi
		}&
		done;
		wait;
	}
	done;
	
	if [ $(($i % 1)) -eq 0 ]
	then
		echo "$i"
	fi
}
done;
rm kvraft.test