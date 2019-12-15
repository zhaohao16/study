#!/bin/bash
export GOPATH="/data/zhaohao/6.824"
export PATH="$PATH"
rm res -rf
mkdir res
go test -c -o shardmaster.test

test_list=(
	TestBasic
	TestMulti
	)
for i in `seq 100`;do
{ 
	for j in ${test_list[@]}; do
	{
		for z in `seq 50`;do
		{
			./shardmaster.test -test.v -test.run $j &> ./res/$z
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
rm shardmaster.test