#!/bin/bash
export GOPATH="/home/zhao/gowork/src/study/mit/mit6.824_2018"
export PATH="$PATH"
rm res -rf
mkdir res
go test -c -o shardkv.test

test_list=(
	TestStaticShards
	TestJoinLeave
	TestSnapshot
	TestMissChange
	TestConcurrent1
	TestConcurrent2
	TestUnreliable1
	TestUnreliable2
	TestUnreliable3
	TestChallenge1Delete
	TestChallenge1Concurrent
	TestChallenge2Unaffected
	TestChallenge2Partial

	)
for i in `seq 100`;do
{ 
	for j in ${test_list[@]}; do
	{
		for z in `seq 10`;do
		{
			./shardkv.test -test.v -test.run $j &> ./res/$z
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
rm shardkv.test