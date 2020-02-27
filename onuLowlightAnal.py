#coding=utf-8
import pandas as pd
import  xml.dom.minidom
import json
from xml.etree import ElementTree as ET
import os
import sys
import datetime, time
import logging
from mysqlProcess import mysqlProcess
from pyLog import Pylog

class onuLowlightAnal(mysqlProcess,Pylog):

    def __init__(self):
        mysqlProcess.__init__(self);
        Pylog.__init__(self);

    def getUnixTime(self, strTime):
        d = datetime.datetime.strptime(strTime, "%Y%m%d%H%M%S")
        d = time.mktime(d.timetuple())
        return (int)(d)

    def read_XZziguanData(self):
        sourceFileName = '/var/XZdataProcess/FamilySupportProcess/result/XZziguanBaseData.csv';
        df=pd.read_csv(sourceFileName,header=None,sep=',',low_memory=False,names=['cityName','districtName','oltIp'])
        df.drop_duplicates(subset=['oltIp'],keep='first',inplace=True)
        print ('get XZziguanData success')
        return df

    def read_OLT(self,sourceFileName):
        df=pd.read_csv(sourceFileName,header=None,sep='+',low_memory=False,names=[1,2,3,4,5,6,7,8,9,10])
        cols=[4,8,10]
        df=df.loc[:,cols].rename(columns={4 : 'oltNativeName',8 : 'oltIp',10 : 'oltrmUID'})
        df.drop_duplicates(subset=['oltIp'],keep='first',inplace=True)
        print ('get read_OLT success')
        return df

    def read_PRT(self,sourceFileName):
        df=pd.read_csv(sourceFileName,header=None,sep='+',low_memory=False,names=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18])
        cols=[4,7,14,17,18]
        df=df.loc[:,cols].rename(columns={4 : 'oltrmUID',7 : 'oltPortNativeName',14 : 'portRate',17 : 'isUpLinkPort',18 : 'oltPortrmUID'})
        df.drop_duplicates(subset=['oltPortrmUID'],keep='first',inplace=True)
        print ('get read_PRT success')
        return df

    def read_ONU(self,sourceFileName):
        df=pd.read_csv(sourceFileName,header=None,sep='+',low_memory=False,names=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23])
        cols=[3,4,6,7,9,12,13,14,15,23]
        df=df.loc[:,cols].rename(columns={3 : 'vendorName',4 : 'onuNativeName',6 : 'oltPortrmUID',7 : 'devIP',9 : 'onuType',12 : 'onuNo',13 : 'onuSN',14 : 'authType',15 : 'authValue',23 : 'onurmUID'})
        df.drop_duplicates(subset=['onurmUID'],keep='first',inplace=True)
        print ('get read_ONU success')
        return df

    def pmOMUData(self,path):
        OMUDataDF = pd.DataFrame() 
        dirs = os.listdir(path)
        for dir in dirs:
            sourceFileName = path +  dir;
            if 'OMU' in sourceFileName:
                names = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]
                OMUDataDF = pd.read_csv(sourceFileName,header = 1, names=names, sep='|', encoding='utf8', low_memory=False)
                cols=[3,4,9,10]
                OMUDataDF=OMUDataDF.loc[:,cols];
                OMUDataDF=OMUDataDF.rename(columns={3 : 'omu_oltIp',4 : 'omu_ponPort',9 : 'rxPower',10 : 'txPower'});
        return OMUDataDF

    def ziGuanOltData(self,allDataPath):
        #生成资管和OLT和oltPort关联数据
        dirs = os.listdir(allDataPath)
        dfOLTAll = pd.DataFrame() 
        dfPRTAll = pd.DataFrame() 
        dfONUAll = pd.DataFrame() 
        for dir in dirs:
            sourceFileName = allDataPath +  dir;
            if 'OLT' in sourceFileName:
                dfOLT = self.read_OLT(sourceFileName)
                dfOLTAll = pd.concat([dfOLTAll,dfOLT])
                dfOLTAll.drop_duplicates(subset=['oltIp'],keep='first',inplace=True)
            if 'PRT' in sourceFileName:
                dfPRT = self.read_PRT(sourceFileName)
                dfPRTAll = pd.concat([dfPRTAll,dfPRT])
                dfPRTAll.drop_duplicates(subset=['oltPortrmUID'],keep='first',inplace=True)
            if 'ONU' in sourceFileName:
                dfONU = self.read_ONU(sourceFileName)
                dfONUAll = pd.concat([dfONUAll,dfONU])
                dfONUAll.drop_duplicates(subset=['onurmUID'],keep='first',inplace=True)
        

        dfALL = pd.merge(dfONUAll,dfPRTAll,how="left",on='oltPortrmUID');
        dfALL = pd.merge(dfALL,dfOLTAll,how="left",on='oltrmUID');
        dfziguanData = self.read_XZziguanData()
        dfALL = pd.merge(dfALL,dfziguanData,how="left",on='oltIp');

        return dfALL

    def mainProcess(self):
        dataTime = sys.argv[1]
        #分别处理三个厂家
        allDataPath='/var/XZdataProcess/FamilySupportProcess/originalData/allData/'
        vendorNameList=['ZTE','FH','HW']
        for vendorName in vendorNameList:
             if 'ZTE' in vendorName:       
                 path = '/var/XZdataProcess/FamilySupportProcess/originalData/ZTECM/'
                 os.system('rm -f  %s*' % (path))
                 pmPath = '/var/ftp/pondata/zhongxing/'
                 os.system('cp  %s*OMU*%s*.csv  %s' % (pmPath,dataTime,path))
                 dfZTEOMU = self.pmOMUData(path)
             if 'FH' in vendorName:      
                 path = '/var/XZdataProcess/FamilySupportProcess/originalData/FHCM/'
                 os.system('rm -f  %s*' % (path))
                 pmPath = '/var/ftp/pondata/fenghuo/'
                 os.system('cp  %s*OMU*%s*.csv  %s' % (pmPath,dataTime,path))
                 dfFHOMU = self.pmOMUData(path)
             if 'HW' in vendorName:        
                 path = '/var/XZdataProcess/FamilySupportProcess/originalData/HWCM/'
                 os.system('rm -f  %s*' % (path))
                 pmPath = '/var/ftp/pondata/huawei/'
                 os.system('cp  %s*OMU*%s*.csv  %s' % (pmPath,dataTime,path))
                 dfHWOMU = self.pmOMUData(path)

        dfOMUALL = pd.concat([dfZTEOMU,dfFHOMU,dfHWOMU])
        dfOMUALL['ip_port'] =  dfOMUALL['omu_oltIp'] + '-' + dfOMUALL['omu_ponPort']
        dfOMUALL.drop_duplicates(subset=['ip_port'],keep='first',inplace=True)

        dfALL = self.ziGuanOltData(allDataPath)
        dfALL['ip_port'] =  dfALL['oltIp'] + '-' + dfALL['oltPortNativeName']
        #dfALL.drop_duplicates(subset=['onuNativeName'],keep='first',inplace=True)
        dfALL.drop_duplicates(subset=['onurmUID'],keep='first',inplace=True)

        dfALL = pd.merge(dfALL,dfOMUALL,how="left",on=['ip_port']);

        cols=['rxPower', 'txPower', 'ip_port', 'vendorName', 'onuNativeName', 'oltPortrmUID',  'onuType', 'onuNo', 'onuSN', 'authType', 'authValue', 'onurmUID', 'oltrmUID', 'oltPortNativeName',  'oltNativeName', 'oltIp', 'cityName', 'districtName']    
        dfALL=dfALL.loc[:,cols];

        #打印设置
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        print (dfALL.tail(30))
        #print (dfALL)
        print (len(dfALL))
        print ('dfALL : ',list(dfALL.columns))

        # 插入时间
        dataUnixTime = self.getUnixTime(dataTime)
        dfALL.insert(0,'reportTime',dataUnixTime) 
        # 插入index
        dfALL.insert(0,'id',0)
        fileNameCsv = allDataPath + 'onuLowlightAnal.csv'
        dfALL.to_csv(fileNameCsv,sep='+',index=False,header=False,encoding = 'utf-8')
        self.InsertData(fileNameCsv, 'onuLowlightAnal','+')


if __name__ == "__main__":
    while True:
          test = onuLowlightAnal();
          test.mainProcess();
          break;


