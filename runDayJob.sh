#!/bin/bash

D1=$(date -d now +%s)
dataPath='/var/XZdataProcess/FamilySupportProcess'
mysqlCommd="mysql -uroot -proot -f XizangMobile -e"

#给数据库表添加分区和删除旧分区
${dataPath}/dataProcess/timePartition.sh

if [[ -z $1 ]];
then
    currentTime=$(date "+%Y%m%d" -d  '-1 days')
    echo ${currentTime}
    echo "固定时间"
    todayTime=$(date "+%Y%m%d")
else
    currentTime=$1
    echo ${currentTime}
    echo "传参时间"
    todayTime=$2
fi

dataTime2=$(date -d "$currentTime" +%s)
echo ${dataTime2}

todayUnixTime=$(date -d "$todayTime" +%s)
echo ${todayUnixTime}

#下载源文件
${dataPath}/dataProcess/getFTPData.sh ${currentTime}

#处理基数据
cd ${dataPath}/pyCode/
python3 readXML.py ${dataTime2}
python3 ponNetworkResourceTree.py ${dataTime2}
python3 FBPerIndexNotice.py ${currentTime}000000 ${todayTime}000000

D2=$(date -d now +%s)
timex=$(($D2-$D1))
echo "脚本处理时间为：${timex}"