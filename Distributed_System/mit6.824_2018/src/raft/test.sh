#!/bin/bash
export GOPATH="/home/zhaohao/6.824_2018"
export PATH="$PATH"
rm res -rf
mkdir res
go test -c -o raft.test

test_list=(
	TestInitialElection2A 
	TestReElection2A 
	TestBasicAgree2B 
	TestFailAgree2B 
	TestFailNoAgree2B 
	TestConcurrentStarts2B 
	TestRejoin2B 
	TestBackup2B 
	TestCount2B
	TestPersist12C
	TestPersist22C
	TestPersist32C
	TestFigure82C
	TestUnreliableAgree2C
	TestFigure8Unreliable2C
	TestReliableChurn2C 
	TestUnreliableChurn2C
	)
for i in `seq 100`;do
{ 
	for j in ${test_list[@]}; do
	{
		for z in `seq 50`;do
		{
			./raft.test -test.v -test.run $j &> ./res/$z
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
rm raft.test