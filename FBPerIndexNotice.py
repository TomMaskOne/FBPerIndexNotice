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

class FBPerIndexNotice(mysqlProcess,Pylog):

    def __init__(self):
        mysqlProcess.__init__(self);
        Pylog.__init__(self);

    def getUnixTime(self, strTime):
        d = datetime.datetime.strptime(strTime, "%Y%m%d%H%M%S")
        d = time.mktime(d.timetuple())
        return (int)(d)

    def mainProcess(self):
        beginTime = sys.argv[1]
        beginUnixTime = self.getUnixTime(beginTime)
        endTime = sys.argv[2]
        endUnixTime = self.getUnixTime(endTime)
        #分别处理七个地市
        cityNameList=['拉萨','山南','日喀则','昌都','林芝','那曲','阿里']
        for cityName in cityNameList:
            self.updateData("insert into FBPerIndexNotice select null,%s as reportTime,'%s' as cityName,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null;" % (beginUnixTime,cityName))
            self.updateData("update FBPerIndexNotice b,(select a.cityName,count(*) as oltNum from (select cityName from ponNetworkResourceTree where cityName= '%s' and reportTime>=%s and reportTime<%s group by oltrmUID)a)c set b.oltNum=c.oltNum where b.cityName=c.cityName and b.reportTime=%s;" % (cityName,beginUnixTime,endUnixTime,beginUnixTime))
            self.updateData("update FBPerIndexNotice b,(select a.cityName,count(*) as ponNum from (select cityName from ponNetworkResourceTree where cityName= '%s' and reportTime>=%s and reportTime<%s group by ip_port)a)c set b.ponNum=c.ponNum where b.cityName=c.cityName and b.reportTime=%s;" % (cityName,beginUnixTime,endUnixTime,beginUnixTime))
            self.updateData("update FBPerIndexNotice b,(select a.cityName,count(*) as onuNum from (select cityName from ponNetworkResourceTree where cityName= '%s' and reportTime>=%s and reportTime<%s group by onurmUID)a)c set b.onuNum=c.onuNum where b.cityName=c.cityName and b.reportTime=%s;" % (cityName,beginUnixTime,endUnixTime,beginUnixTime))
            self.updateData("update FBPerIndexNotice b,(select a.cityName,sum(a.recSpeed)/sum(a.portRate*1024) as oltUplinkMaxBWRate from (select cityName,max(recSpeed) as recSpeed,max(portRate) as portRate from oltUpperCoupletRate where cityName='%s' and reportTime>=%s and reportTime<%s group by oltrmUID)a)c set b.oltUplinkMaxBWRate=c.oltUplinkMaxBWRate where b.cityName=c.cityName and b.reportTime=%s;" % (cityName,beginUnixTime,endUnixTime,beginUnixTime))
            self.updateData("update FBPerIndexNotice b,(select  a.cityName,count(*) as oltUplinkMaxBWSevenNum from (select cityName,max(recSpeed)/max(portRate*1024) as maxBWRate from oltUpperCoupletRate where cityName='%s' and reportTime>=%s and reportTime<%s group by oltrmUID)a where a.maxBWRate>=0.7)c set b.oltUplinkMaxBWSevenNum=c.oltUplinkMaxBWSevenNum where b.cityName=c.cityName and b.reportTime=%s;" % (cityName,beginUnixTime,endUnixTime,beginUnixTime))
            self.updateData("update FBPerIndexNotice b,(select a.cityName,count(*) as oltUplinkMaxBWNineNum from (select cityName,max(recSpeed)/max(portRate*1024) as maxBWRate from oltUpperCoupletRate where cityName='%s' and reportTime>=%s and reportTime<%s group by oltrmUID)a where a.maxBWRate>=0.9)c set b.oltUplinkMaxBWNineNum=c.oltUplinkMaxBWNineNum where b.cityName=c.cityName and b.reportTime=%s;" % (cityName,beginUnixTime,endUnixTime,beginUnixTime))
            self.updateData("update FBPerIndexNotice b,(select a.cityName,count(*) as oltUplinkWeakLightNum from (select cityName  from oltUpperCoupletRate where cityName='%s' and rxPower<-15 and reportTime>=%s and reportTime<%s group by oltrmUID)a)c set b.oltUplinkWeakLightNum=c.oltUplinkWeakLightNum where b.cityName=c.cityName and b.reportTime=%s;" % (cityName,beginUnixTime,endUnixTime,beginUnixTime))
            self.updateData("update FBPerIndexNotice b,(select a.cityName,count(*) as onuWeakLightNum from (select cityName  from onuLowlightAnal where cityName='%s' and (rxPower>-8 or rxPower<-25) and reportTime>=%s and reportTime<%s group by onurmUID)a)c set b.onuWeakLightNum=c.onuWeakLightNum where b.cityName=c.cityName and b.reportTime=%s;" % (cityName,beginUnixTime,endUnixTime,beginUnixTime))
            self.updateData("update FBPerIndexNotice b,(select a.cityName,count(*) as ponHeavyLoadNum from (select cityName  from ponNetworkResourceTree where cityName='%s' and ponUserNum>64  and reportTime>=%s and reportTime<%s  group by ip_port)a)c set b.ponHeavyLoadNum=c.ponHeavyLoadNum where b.cityName=c.cityName and b.reportTime=%s;" % (cityName,beginUnixTime,endUnixTime,beginUnixTime))
            self.updateData("update FBPerIndexNotice b,(select a.cityName,count(*) as oltHangDownOnuTenNum from (select cityName  from ponNetworkResourceTree where cityName='%s' and oltUserNum<10  and reportTime>=%s and reportTime<%s  group by oltrmUID)a)c set b.oltHangDownOnuTenNum=c.oltHangDownOnuTenNum where b.cityName=c.cityName and b.reportTime=%s;" % (cityName,beginUnixTime,endUnixTime,beginUnixTime))
            self.updateData("update FBPerIndexNotice b,(select a.cityName,count(*) as oltHangDownOnuThhNum from (select cityName  from ponNetworkResourceTree where cityName='%s' and oltUserNum<300  and reportTime>=%s and reportTime<%s  group by oltrmUID)a)c set b.oltHangDownOnuThhNum=c.oltHangDownOnuThhNum where b.cityName=c.cityName and b.reportTime=%s;" % (cityName,beginUnixTime,endUnixTime,beginUnixTime))

        self.updateData("update FBPerIndexNotice set oltUplinkMaxBWSevenRate=oltUplinkMaxBWSevenNum/oltNum where reportTime=%s;" % (beginUnixTime))
        self.updateData("update FBPerIndexNotice set oltUplinkMaxBWNineRate=oltUplinkMaxBWNineNum/oltNum where reportTime=%s;" % (beginUnixTime))
        self.updateData("update FBPerIndexNotice set oltUplinkWeakLightRate=oltUplinkWeakLightNum/oltNum where reportTime=%s;" % (beginUnixTime))
        self.updateData("update FBPerIndexNotice set onuWeakLightRate=onuWeakLightNum/onuNum where reportTime=%s;" % (beginUnixTime))
        self.updateData("update FBPerIndexNotice set ponHeavyLoadRate=ponHeavyLoadNum/ponNum where reportTime=%s;" % (beginUnixTime))
        self.updateData("update FBPerIndexNotice set oltHangDownOnuTenRate=oltHangDownOnuTenNum/oltNum where reportTime=%s;" % (beginUnixTime))
        self.updateData("update FBPerIndexNotice set oltHangDownOnuThhRate=oltHangDownOnuThhNum/oltNum where reportTime=%s;" % (beginUnixTime))

if __name__ == "__main__":
    while True:
          test = FBPerIndexNotice();
          test.mainProcess();
          break;


