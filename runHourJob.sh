#!/bin/bash

D1=$(date -d now +%s)
dataPath='/var/XZdataProcess/FamilySupportProcess'
mysqlCommd="mysql -uroot -proot -f XizangMobile -e"
#清理CM文件夹文件
rm -rf ${dataPath}/originalData/ZTECM/*
rm -rf ${dataPath}/originalData/FHCM/*
rm -rf ${dataPath}/originalData/HWCM/*

if [[ -z $1 ]];
then
    currentTime=$(date "+%Y%m%d%H" -d  '-2 hours')
    echo ${currentTime}
    echo "固定时间"
else
    currentTime=$1
    echo ${currentTime}
    echo "传参时间"
fi


#OLT上联口分析
cd ${dataPath}/pyCode/
python3 oltUpperCouplet.py ${currentTime}
#ONU弱光分析
python3 onuLowlightAnal.py ${currentTime}0000

D2=$(date -d now +%s)
timex=$(($D2-$D1))
echo "脚本处理时间为：${timex}"