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

class dataProcess(mysqlProcess,Pylog):

    def __init__(self):
        mysqlProcess.__init__(self);
        Pylog.__init__(self);

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

    #统计olt下挂用户数
    def countOltUsernum(self,dfALL):
        dfOltUserNum = dfALL.groupby(["oltrmUID"]).size()
        dfOltUserNum = pd.DataFrame({'oltUserNum':dfOltUserNum})        
        dfOltUserNum.reset_index(inplace=True)
        return dfOltUserNum

    #统计pon口下挂用户数
    def countPonUsernum(self,dfALL):
        dfPonUserNum = dfALL.groupby(["oltPortrmUID"]).size()
        dfPonUserNum = pd.DataFrame({'ponUserNum':dfPonUserNum})        
        dfPonUserNum.reset_index(inplace=True)
        return dfPonUserNum

    def exportCsv(self,allDataPath):
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
        dfALL.drop_duplicates(subset=['onurmUID'],keep='first',inplace=True)

        #统计OLT下挂用户数分析,PON口下挂用户数分析
        dfOltUserNum = self.countOltUsernum(dfALL)
        dfPonUserNum = self.countPonUsernum(dfALL)
        dfALL = pd.merge(dfALL,dfOltUserNum,how="left",on='oltrmUID').fillna(0)
        dfALL = pd.merge(dfALL,dfPonUserNum,how="left",on='oltPortrmUID').fillna(0)
        dfALL['ip_port'] = dfALL['oltIp'] + '-' + dfALL['oltPortNativeName'] 
        cols=['oltNativeName', 'oltIp', 'oltrmUID', 'cityName', 'districtName', 'oltPortNativeName', 'portRate', 'isUpLinkPort', 'oltPortrmUID', 'vendorName', 'onuNativeName', 'devIP', 'onuType', 'onuNo', 'onuSN', 'authType', 'authValue', 'onurmUID', 'oltUserNum', 'ponUserNum','ip_port']    
        dfALL=dfALL.loc[:,cols];
        # 插入时间
        dataUnixTime = sys.argv[1]
        dfALL.insert(0,'reportTime',dataUnixTime) 
        # 插入index
        dfALL.insert(0,'id',0)
        fileNameCsv = allDataPath + 'ponNetworkResourceTree.csv'
        dfALL.to_csv(fileNameCsv,sep='+',index=False,header=False,encoding = 'utf-8')
        self.InsertData(fileNameCsv, 'ponNetworkResourceTree','+')
        self.updateData("update ponNetworkResourceTree set cityName='',districtName=''  where cityName='0' and reportTime=%s;" % (dataUnixTime))

        #打印设置
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        print (dfALL.tail())
        print (len(dfALL))
        print ('dfALL : ',list(dfALL.columns))


    def mainProcess(self):
        #分别处理三个厂家
        allDataPath='/var/XZdataProcess/FamilySupportProcess/originalData/allData/'
        os.system('rm -f  %s*' % (allDataPath))
        vendorNameList=['ZTE','FH','HW']
        for vendorName in vendorNameList:
             if 'ZTE' in vendorName:       
                 path = '/var/XZdataProcess/FamilySupportProcess/originalData/ZTE/'
                 os.system('mv  %s*.csv  %s' % (path,allDataPath))
             if 'FH' in vendorName:      
                 path = '/var/XZdataProcess/FamilySupportProcess/originalData/FH/'
                 os.system('mv  %s*.csv  %s' % (path,allDataPath))
             if 'HW' in vendorName:        
                 path = '/var/XZdataProcess/FamilySupportProcess/originalData/HW/'
                 os.system('mv  %s*.csv  %s' % (path,allDataPath))
        self.exportCsv(allDataPath)
        

if __name__ == "__main__":
    while True:
          test = dataProcess();
          test.mainProcess();
          break;

