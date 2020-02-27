
#ONU弱光分析
def getOnuLowlightAnal(request):
    try:
        start_time = time.time()
        logger=logAlarm()
        json_Dirt={}
        nErrorCode = 0
        strTableName=""
        strColumn="*"
        strCondition=""
        strConditionChage = ""
        result=[]
        resultTotal=[]
        resultAll={}
        #获取参数
        if request.method == "GET":
            beginTime = request.GET.get('beginTime')
            endTime = request.GET.get('endTime')
            cityName = request.GET.get('cityName')
            districtName = request.GET.get('districtName')
            oltNativeName = request.GET.get('oltNativeName')
            vendorName = request.GET.get('vendorName')
            ip_port = request.GET.get('ip_port')
            onuSN = request.GET.get('onuSN')
            queryField = request.GET.get('queryField','rxPower>-8 or rxPower<-25')
            start = request.GET.get('start',0)
            limit = request.GET.get('limit',50)
            sortName = request.GET.get('sortName','rxPower')
            isAsc = request.GET.get('isAsc','desc')
            
            
            strTableName = "onuLowlightAnal"
            strColumn = " reportTime,cityName,districtName,vendorName,onuNativeName,onuSN,onuType,oltNativeName,oltIp,oltPortNativeName,rxPower "

            strCondition = " reportTime>=%s and reportTime<%s and  (%s)  order by %s %s limit %s, %s " % (beginTime,endTime,queryField,sortName,isAsc,start,limit)
            str_CountCondition = " reportTime>=%s and reportTime<%s  and  (%s) " % (beginTime,endTime,queryField)
            strConditionresult = " reportTime=(select max(reportTime) from %s) and (%s)   order by %s %s limit %s, %s " % (strTableName,queryField,sortName,isAsc,start,limit)
            str_CountConditionresult = " reportTime=(select max(reportTime) from %s)  and (%s) " % (strTableName,queryField)

            if cityName=='All':
                strConditionChage = ""
                
            if cityName and cityName!='All':
                strConditionChage = strConditionChage + " cityName='%s' and  " % (cityName)

            if districtName:
                strConditionChage = strConditionChage + " districtName='%s' and  " % (districtName)
   
            if oltNativeName:
                strConditionChage = strConditionChage + " oltNativeName='%s' and  " % (oltNativeName)

            if vendorName:
                strConditionChage = strConditionChage + " vendorName='%s' and " % (vendorName)

            if ip_port:
                strConditionChage = strConditionChage + " ip_port='%s' and " % (ip_port)

            if onuSN:
                strConditionChage = strConditionChage + " onuSN='%s' and " % (onuSN)
              
        strCondition = strConditionChage + strCondition
        str_CountCondition =  strConditionChage + str_CountCondition
        strConditionresult = strConditionChage + strConditionresult
        str_CountConditionresult = strConditionChage + str_CountConditionresult
        result,totalCount,nErrorCode=getPonDetailsResult(logger,strTableName,strColumn,strCondition,str_CountCondition,strConditionresult,str_CountConditionresult)

        json_Dirt["rows"]=result
        json_Dirt["totalCount"]=totalCount
        json_Dirt["errorCode"]=nErrorCode
        json_Dirt["totalQueryTime"]=time.time()-start_time
    except Exception as e:
        nErrorCode = 1
        logger.info("error is:" % (e))    
        json_Dirt["rows"]=[]
        json_Dirt["totalCount"]=0
        json_Dirt["errorCode"]=nErrorCode
        json_Dirt["totalQueryTime"]=time.time()-start_time

    return HttpResponse(json.dumps(json_Dirt),content_type="application/json; charset=utf-8")
    
    