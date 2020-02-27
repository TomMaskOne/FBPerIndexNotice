#!/bin/bash

ftpGetFileFun()
{
server=${1}
user=${2}
password=${3}
lpath=${4}
path=${5}
ftp -n -i $server <<file
user $user $password
binary 
passive
lcd ${lpath}
cd ${path}
mget *${6}*
close
bye
file
}

#下载HW数据
getHWData()
{
    server='10.237.32.150'
    username='duandaoduan'
    password='Duan@#$qlz456!'
    localPath=/var/XZdataProcess/FamilySupportProcess/originalData/HW
    remote_dir=/opt/backup/ftpboot/ftproot/LS/CS/HW/Huawei_U2000/CM/${1}
    fileNameOLT='OLT'
    fileNameONU='ONU'
    fileNamePRT='PRT'

    ftpGetFileFun ${server} ${username} ${password} ${localPath} ${remote_dir} ${fileNameOLT}
    ftpGetFileFun ${server} ${username} ${password} ${localPath} ${remote_dir} ${fileNameONU}
    ftpGetFileFun ${server} ${username} ${password} ${localPath} ${remote_dir} ${fileNamePRT}
}

#下载ZTE数据
getZTEData()
{
    server='10.233.234.130'
    username='zte'
    password='zte@123'
    localPath=/var/XZdataProcess/FamilySupportProcess/originalData/ZTE
    remote_dir=/CS/ZT/OMC/CM/${1}
    fileNameOLT='OLT'
    fileNameONU='ONU'
    fileNamePRT='PRT'

    ftpGetFileFun ${server} ${username} ${password} ${localPath} ${remote_dir} ${fileNameOLT}
    ftpGetFileFun ${server} ${username} ${password} ${localPath} ${remote_dir} ${fileNameONU}
    ftpGetFileFun ${server} ${username} ${password} ${localPath} ${remote_dir} ${fileNamePRT}
}

#下载FH数据
getFHData()
{
    server='10.237.32.101'
    username='1'
    password='1'
    localPath=/var/XZdataProcess/FamilySupportProcess/originalData/FH
    remote_dir=/XZ/CS/FH/FH_OMC1/CM/${1}
    fileNameOLT='OLT'
    fileNameONU='ONU'
    fileNamePRT='PRT'

    ftpGetFileFun ${server} ${username} ${password} ${localPath} ${remote_dir} ${fileNameOLT}
    ftpGetFileFun ${server} ${username} ${password} ${localPath} ${remote_dir} ${fileNameONU}
    ftpGetFileFun ${server} ${username} ${password} ${localPath} ${remote_dir} ${fileNamePRT}

}

Main()
{
    echo "文件开始清理"
    resultPath=/var/XZdataProcess/FamilySupportProcess/result/
    ZTEPath=/var/XZdataProcess/FamilySupportProcess/originalData/ZTE/
    HWPath=/var/XZdataProcess/FamilySupportProcess/originalData/HW/
    FHPath=/var/XZdataProcess/FamilySupportProcess/originalData/FH/
    rm -rf ${ZTEPath}*
    rm -rf ${HWPath}*
    rm -rf ${FHPath}*
    echo "文件清理完成"

    echo "文件开始下载"
    getHWData ${1}
    getFHData ${1}

    #中兴文件需要下载之前的数据
    getZTEData ${1}
    count=`ls ${ZTEPath}|wc -l`
    if [ $count -eq 0 ]; then
        echo "文件下载为空，copy上一期数据"
        cp ${resultPath}*.zip ${ZTEPath}
    else
        echo "文件下载成功，保留本期数据到result"
        rm -rf ${resultPath}*.zip
        cp ${ZTEPath}*.zip ${resultPath}
    fi    
    echo "文件下载完成"

}

Main ${1}

