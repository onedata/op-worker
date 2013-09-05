#!/bin/bash

Visible=non
DoneSum=0
AllSum=0
CompileErrors=0

while read line
do
  if grep -q "Testing .*: Starting test" <<<"$line"
  then
    Visible=ok
  fi

  if grep -q "CWD set to: .*" <<<"$line"
  then
    Visible=non
  fi

  if grep -q "Common Test .* starting .*" <<<"$line"
  then
    Visible=non
  fi

  if [ "$Visible" == ok ]
  then
    echo "$line"
  fi

  if grep -q "Recompile: .*" <<<"$line"
  then
    Visible=ok
  fi

  if grep -q "Testing .*: TEST COMPLETE" <<<"$line"
  then
    Visible=non

    DoneStart=`expr match "$line" "Testing .*: TEST COMPLETE, "`
    DoneStopTmp=`expr match "$line" "Testing .*: TEST COMPLETE, .* ok"`
    DoneStop=`expr $DoneStopTmp - 3`
    DoneLength=`expr $DoneStop - $DoneStart`
    Done=${line:$DoneStart:$DoneLength}
    DoneSum=`expr $DoneSum + $Done`

    AllStart=`expr match "$line" "Testing .*: TEST COMPLETE, .*failed of "`
    AllStopTmp=`expr match "$line" "Testing .*: TEST COMPLETE, .*failed of .* test cases"`
    AllStop=`expr $AllStopTmp - 10`
    AllLength=`expr $AllStop - $AllStart`
    All=${line:$AllStart:$AllLength}
    AllSum=`expr $AllSum + $All`
  fi

  if grep -q ".*{error,make_failed}.*" <<<"$line"
  then
    CompileErrors=`expr $CompileErrors + 1`
  fi
done

echo "Distributed tests OK: $DoneSum, all: $AllSum, Compile errors: $CompileErrors"

if [ $CompileErrors -ne 0 ]
then
  exit 1
fi