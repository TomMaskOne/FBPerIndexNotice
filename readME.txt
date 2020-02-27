此模块为PON数据报表，下面对各文件作解释说明：
1，dataProcess下两个执行文件
runHourJob.sh，小时处理脚本。支持传参和不传参，不传参默认处理上一个小时数据，传参格式为 ./runHourJob.sh 2020022512
runDayJob.sh，  天处理脚本。支持传参和不传参，不传参默认处理上昨天数据，传参格式为 ./runDayJob.sh 20200225 20200226
