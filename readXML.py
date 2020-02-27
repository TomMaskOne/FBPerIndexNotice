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

class readXMLFile(mysqlProcess,Pylog):

    def __init__(self):
        mysqlProcess.__init__(self);
        Pylog.__init__(self);
    
    def readDeviceInfo(self,fileName):
        json_Dirt=[]
        listDeviceInfo=[]
        tree = ET.parse(fileName)
        root = tree.getroot()
        for child in root:
            for i in child:
                for j in i:
                    for a,h in enumerate(j,2):
                        json_Dirt.append(h.text)
                    if json_Dirt:
                        listDeviceInfo.append(json_Dirt)
                        json_Dirt=[]
        self.logger.info('[' + fileName + ']' + '提取设备信息字段完成')
        return listDeviceInfo

    def readRMUID(self,fileName):
        json_DirtRM=[]
        listRM=[]
        dom = xml.dom.minidom.parse(fileName)
        root = dom.documentElement
        resp=dom.getElementsByTagName('Object')
        for b,i in enumerate(resp,1):
            json_DirtRM.append(i.getAttribute("rmUID"))
            if json_DirtRM:
               listRM.append(json_DirtRM)
               json_DirtRM=[]
        self.logger.info('[' + fileName + ']' + '提取ruuid字段完成')
        return listRM


    def exportCsv(self,path,vendorName):
        dirs = os.listdir(path)
        #解压文件
        for dir in dirs:
            fileName=path+dir
            os.system('unzip -d %s %s ' % (path,fileName))
            os.system('rm -rf %s ' % (fileName))
            self.logger.info('[' + dir + ']' + '文件解压完成')

        
        #读取XML生成CSV
        dirs = os.listdir(path)
        for dir in dirs:
            fileName=path+dir
            listLast=[]
            listTotal=[]
            listDeviceInfo=self.readDeviceInfo(fileName)
            listRM=self.readRMUID(fileName)
            for a,h in enumerate(listDeviceInfo):
                listLast=h+listRM[a]
                listTotal.append(listLast)
            if 'OLT' in fileName:
                fileNameCsv=fileName + '.csv'
                df = pd.DataFrame(listTotal, columns=[1,2,3,4,5,6,7])
                #插入厂家名称
                df.insert(0,'vendorName',vendorName) 
                # 插入时间
                dataUnixTime = sys.argv[1]
                df.insert(0,'reportTime',dataUnixTime) 
                # 插入index
                df.insert(0,'id',0)
                df.to_csv(fileNameCsv,sep='+',index=False,header=False,encoding = 'utf-8')
                self.InsertData(fileNameCsv, 'CMPONOLTBaseData','+')

            if 'ONU' in fileName:
                fileNameCsv=fileName + '.csv'
                df = pd.DataFrame(listTotal, columns=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20])
                #插入厂家名称
                df.insert(0,'vendorName',vendorName) 
                # 插入时间
                dataUnixTime = sys.argv[1]
                df.insert(0,'reportTime',dataUnixTime) 
                # 插入index
                df.insert(0,'id',0)
                df.to_csv(fileNameCsv,sep='+',index=False,header=False,encoding = 'utf-8')
                self.InsertData(fileNameCsv, 'CMPONONUBaseData','+')

            if 'PRT' in fileName:
                fileNameCsv=fileName + '.csv'
                df = pd.DataFrame(listTotal, columns=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15])
                #插入厂家名称
                df.insert(0,'vendorName',vendorName) 
                # 插入时间
                dataUnixTime = sys.argv[1]
                df.insert(0,'reportTime',dataUnixTime) 
                # 插入index
                df.insert(0,'id',0)
                df.to_csv(fileNameCsv,sep='+',index=False,header=False,encoding = 'utf-8')
                self.InsertData(fileNameCsv, 'CMPONPRTBaseData','+')


    def mainProcess(self):
        #分别处理三个厂家
        vendorNameList=['ZTE','FH','HW']
        for vendorName in vendorNameList:
             if 'ZTE' in vendorName:
                 self.logger.info('开始处理中兴数据')        
                 path = '/var/XZdataProcess/FamilySupportProcess/originalData/ZTE/'
                 self.exportCsv(path,'ZTE')
                 self.logger.info('中兴数据处理成功')
             if 'FH' in vendorName:    
                 self.logger.info('开始处理烽火数据')    
                 path = '/var/XZdataProcess/FamilySupportProcess/originalData/FH/'
                 self.exportCsv(path,'FH')
                 self.logger.info('烽火数据处理成功')
             if 'HW' in vendorName:        
                 self.logger.info('开始处理华为数据')
                 path = '/var/XZdataProcess/FamilySupportProcess/originalData/HW/'
                 self.exportCsv(path,'HW')
                 self.logger.info('华为数据处理成功')

if __name__ == "__main__":
    while True:
          test = readXMLFile();
          test.mainProcess();
          break;