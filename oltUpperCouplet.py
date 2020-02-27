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

class oltUpperCouplet(mysqlProcess,Pylog):

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
        cols=[3,4,7,14,17,18]
        df=df.loc[:,cols].rename(columns={3 : 'vendorName',4 : 'oltrmUID',7 : 'oltPortNativeName',14 : 'portRate',17 : 'isUpLinkPort',18 : 'oltPortrmUID'})
        df.drop_duplicates(subset=['oltPortrmUID'],keep='first',inplace=True)
        print ('get read_PRT success')
        return df

    def pmZTEUOOData(self,path):
        UOODataDF = pd.DataFrame() 
        dirs = os.listdir(path)
        for dir in dirs:
            sourceFileName = path +  dir;
            if 'UOO' in sourceFileName:
                names = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21]
                UOODataDF = pd.read_csv(sourceFileName,header = 1, names=names, sep='|', encoding='utf8', low_memory=False)
                cols=[1,4,7,8]
                UOODataDF=UOODataDF.loc[:,cols];
                UOODataDF=UOODataDF.rename(columns={1 : 'oltPortrmUID',4 : 'oltPortId',7 : 'sendSpeed',8 : 'recSpeed'});
                UOODataDF['sendSpeed'] = UOODataDF['sendSpeed'].astype(str).replace('--','0').astype(float)*8/1024/1024/(15*60)
                UOODataDF['recSpeed'] = UOODataDF['recSpeed'].astype(str).replace('--','0').astype(float)*8/1024/1024/(15*60)
        return UOODataDF

    def pmFHUOOData(self,path):
        UOODataDF = pd.DataFrame() 
        dirs = os.listdir(path)
        for dir in dirs:
            sourceFileName = path +  dir;
            if 'UOO' in sourceFileName:
                names = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21]
                UOODataDF = pd.read_csv(sourceFileName,header = 1, names=names, sep='|', encoding='utf8', low_memory=False)
                cols=[1,4,7,8]
                UOODataDF=UOODataDF.loc[:,cols];
                UOODataDF=UOODataDF.rename(columns={1 : 'oltPortrmUID',4 : 'oltPortId',7 : 'sendSpeed',8 : 'recSpeed'});
                UOODataDF['sendSpeed'] = UOODataDF['sendSpeed'].astype(str).replace('--','0').astype(float)*8/1024/1024/(15*60)
                UOODataDF['recSpeed'] = UOODataDF['recSpeed'].astype(str).replace('--','0').astype(float)*8/1024/1024/(15*60)
        return UOODataDF

    def pmHWUOOData(self,path):
        UOODataDF = pd.DataFrame() 
        dirs = os.listdir(path)
        for dir in dirs:
            sourceFileName = path +  dir;
            if 'UOO' in sourceFileName:
                names = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21]
                UOODataDF = pd.read_csv(sourceFileName,header = 1, names=names, sep='|', encoding='utf8', low_memory=False)
                cols=[2,4,7,8]
                UOODataDF=UOODataDF.loc[:,cols];
                UOODataDF=UOODataDF.rename(columns={2 : 'oltPortrmUID',4 : 'oltPortId',7 : 'sendSpeed',8 : 'recSpeed'});
                UOODataDF['sendSpeed'] = UOODataDF['sendSpeed'].astype(str).replace('--','0').astype(float)*8/1024/1024/(15*60)
                UOODataDF['recSpeed'] = UOODataDF['recSpeed'].astype(str).replace('--','0').astype(float)*8/1024/1024/(15*60)
        return UOODataDF

    def pmZTEOMOData(self,path):
        OMODataDF = pd.DataFrame() 
        dirs = os.listdir(path)
        for dir in dirs:
            sourceFileName = path +  dir;
            if 'OMO' in sourceFileName:
                names = [1,2,3,4,5,6,7,8,9]
                OMODataDF = pd.read_csv(sourceFileName,header = 1, names=names, sep='|', encoding='utf8', low_memory=False)
                cols=[1,5,6]
                OMODataDF=OMODataDF.loc[:,cols];
                OMODataDF=OMODataDF.rename(columns={1 : 'oltPortrmUID',5 : 'rxPower',6 : 'txPower'});
        return OMODataDF

    def pmFHOMOData(self,path):
        OMODataDF = pd.DataFrame() 
        dirs = os.listdir(path)
        for dir in dirs:
            sourceFileName = path +  dir;
            if 'OMO' in sourceFileName:
                names = [1,2,3,4,5,6,7,8,9]
                OMODataDF = pd.read_csv(sourceFileName,header = 1, names=names, sep='|', encoding='utf8', low_memory=False)
                cols=[1,5,6]
                OMODataDF=OMODataDF.loc[:,cols];
                OMODataDF=OMODataDF.rename(columns={1 : 'oltPortrmUID',5 : 'rxPower',6 : 'txPower'});
        return OMODataDF

    def pmHWOMOData(self,path):
        OMODataDF = pd.DataFrame() 
        dirs = os.listdir(path)
        for dir in dirs:
            sourceFileName = path +  dir;
            if 'OMO' in sourceFileName:
                names = [1,2,3,4,5,6,7,8,9]
                OMODataDF = pd.read_csv(sourceFileName,header = 1, names=names, sep='|', encoding='utf8', low_memory=False)
                cols=[2,5,6]
                OMODataDF=OMODataDF.loc[:,cols];
                OMODataDF=OMODataDF.rename(columns={2 : 'oltPortrmUID',5 : 'rxPower',6 : 'txPower'});
        return OMODataDF


    def ziGuanOltData(self,allDataPath):
        #生成资管和OLT和oltPort关联数据
        dirs = os.listdir(allDataPath)
        dfOLTAll = pd.DataFrame() 
        dfPRTAll = pd.DataFrame() 
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
        
        dfziguanData = self.read_XZziguanData()
        dfALL = pd.merge(dfOLTAll,dfziguanData,how="left",on='oltIp');
        dfALL.drop_duplicates(subset=['oltrmUID'],keep='first',inplace=True)

        dfALL = pd.merge(dfALL,dfPRTAll,how="left",on='oltrmUID');
        return dfALL

    #选取OLT上联口数据
    def chooseOltUpperCouplet(self,vendorName,dfALL):
        if 'FH' in vendorName:
            dfALL = dfALL[dfALL['vendorName'] == 'FH']
            dfALL = dfALL.query('oltPortId == ["1-1-19-1", "1-1-19-2", "1-1-19-3", "1-1-19-4", "1-1-19-5", "1-1-19-6","1-1-20-1", "1-1-20-2", "1-1-20-3", "1-1-20-4", "1-1-20-5", "1-1-20-6","1-1-9-1", "1-1-9-2", "1-1-9-3", "1-1-9-4","1-1-10-1", "1-1-10-2", "1-1-10-3", "1-1-10-4"]')
        if 'ZTE' in vendorName:
            dfALL = dfALL[dfALL['vendorName'] == 'ZTE']
            dfALL = dfALL.query('oltPortId == ["1-1-19-1", "1-1-19-2", "1-1-19-3", "1-1-19-4","1-1-20-1", "1-1-20-2", "1-1-20-3", "1-1-20-4","1-1-3-1", "1-1-3-2", "1-1-3-3", "1-1-3-4","1-1-10-1", "1-1-10-2", "1-1-10-3", "1-1-10-4"]')
        if 'HW' in vendorName:
            dfALL = dfALL[dfALL['vendorName'] == 'HW']
            dfALL = dfALL.query('oltPortId == ["NA-0-2-0", "NA-0-2-1", "NA-0-2-2", "NA-0-2-3","NA-0-3-0", "NA-0-3-1", "NA-0-3-2", "NA-0-3-3", "NA-0-17-0", "NA-0-17-1","NA-0-18-0", "NA-0-18-1"]')
        return dfALL

    def mainProcess(self,dataTime):     
        #分别处理三个厂家
        allDataPath='/var/XZdataProcess/FamilySupportProcess/originalData/allData/'
        vendorNameList=['ZTE','FH','HW']
        for vendorName in vendorNameList:
             if 'ZTE' in vendorName:       
                 path = '/var/XZdataProcess/FamilySupportProcess/originalData/ZTECM/'
                 os.system('rm -f  %s*' % (path))
                 pmPath = '/var/ftp/pondata/zhongxingMiddle/'
                 os.system('cp  %s*UOO*%s*.csv  %s' % (pmPath,dataTime,path))
                 os.system('cp  %s*OMO*%s*.csv  %s' % (pmPath,dataTime,path))
                 dfZTEUOO = self.pmZTEUOOData(path)
                 dfZTEOMO = self.pmZTEOMOData(path)
             if 'FH' in vendorName:      
                 path = '/var/XZdataProcess/FamilySupportProcess/originalData/FHCM/'
                 os.system('rm -f  %s*' % (path))
                 pmPath = '/var/ftp/pondata/fenghuoMiddle/'
                 os.system('cp  %s*UOO*%s*.csv  %s' % (pmPath,dataTime,path))
                 os.system('cp  %s*OMO*%s*.csv  %s' % (pmPath,dataTime,path))
                 dfFHUOO = self.pmFHUOOData(path)
                 dfFHOMO = self.pmFHOMOData(path)
             if 'HW' in vendorName:        
                 path = '/var/XZdataProcess/FamilySupportProcess/originalData/HWCM/'
                 os.system('rm -f  %s*' % (path))
                 pmPath = '/var/ftp/pondata/huawei/'
                 os.system('cp  %s*UOO*%s*.csv  %s' % (pmPath,dataTime,path))
                 os.system('cp  %s*OMO*%s*.csv  %s' % (pmPath,dataTime,path))
                 dfHWUOO = self.pmHWUOOData(path)
                 dfHWOMO = self.pmHWOMOData(path)

        dfUOOAll = pd.concat([dfZTEUOO,dfFHUOO,dfHWUOO])
        dfUOOAll.drop_duplicates(subset=['oltPortrmUID'],keep='first',inplace=True)

        dfOMOAll = pd.concat([dfZTEOMO,dfFHOMO,dfHWOMO])
        dfOMOAll.drop_duplicates(subset=['oltPortrmUID'],keep='first',inplace=True)

        dfALL = self.ziGuanOltData(allDataPath)
        dfALL.drop_duplicates(subset=['oltPortrmUID'],keep='first',inplace=True)
        
        #关联资管OLT和UOO和OMO数据
        dfALL = pd.merge(dfALL,dfUOOAll,how="left",on='oltPortrmUID');
        dfALL = pd.merge(dfALL,dfOMOAll,how="left",on='oltPortrmUID');
        dfALL['ip_port'] = dfALL['oltIp'] + '-' + dfALL['oltPortNativeName']      
        #dfALL.drop_duplicates(subset=['ip_port'],keep='first',inplace=True)

        dfFHALL = self.chooseOltUpperCouplet('FH',dfALL)
        dfZTEALL = self.chooseOltUpperCouplet('ZTE',dfALL)
        dfHWALL = self.chooseOltUpperCouplet('HW',dfALL)
        dfALL = pd.concat([dfFHALL,dfZTEALL,dfHWALL])

        # 插入时间
        dataUnixTime = self.getUnixTime(dataTime)
        dfALL.insert(0,'reportTime',dataUnixTime) 
        # 插入index
        dfALL.insert(0,'id',0)
        fileNameCsv = allDataPath + 'oltUpperCouplet.csv'
        dfALL.to_csv(fileNameCsv,sep='+',index=False,header=False,encoding = 'utf-8')
        self.InsertData(fileNameCsv, 'oltUpperCouplet','+')
        self.updateData('insert into oltUpperCoupletRate select null,reportTime,oltNativeName,oltIp,oltrmUID,cityName,districtName,vendorName,oltPortNativeName,portRate,isUpLinkPort,oltPortrmUID,oltPortId,sendSpeed,recSpeed,rxPower,txPower,ip_port,(recSpeed/(portRate*1024)) as BWRate from oltUpperCouplet;')
        self.updateData('truncate oltUpperCouplet')

        #打印设置
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        print (dfALL.tail(30))
        #print (dfALL)
        print (len(dfALL))
        print ('dfALL : ',list(dfALL.columns))

    def mainPro(self):
        currentTime = sys.argv[1]
        timeList=['0000','1500','3000','4500']
        for timeStr in timeList:
            dataTime = currentTime+timeStr
            self.mainProcess(dataTime)


if __name__ == "__main__":
    while True:
          test = oltUpperCouplet();
          test.mainPro();
          break;



