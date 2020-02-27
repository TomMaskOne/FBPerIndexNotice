#!/bin/bash

D1=$(date -d now +%s)
mysqlCommd="mysql -uroot -proot -f XizangMobile -e"

TIME_MONTH_AGO=$(date "+%Y%m%d" -d  '-30 days')
TIME_10days_AGO=$(date "+%Y%m%d" -d  '-10 days')
TIME_1days_AGO=$(date "+%Y%m%d" -d  '-1 days')
TIME_TODAY=$(date "+%Y%m%d")
TIME_TOMORROW=$(date "+%Y%m%d" -d  '+1 days')

TIME_UNIX_TODAY=$(date -d "$TIME_TODAY" +%s)
echo ${TIME_UNIX_TODAY}
TIME_UNIX_TOMORROW=$(date -d "$TIME_TOMORROW" +%s)
echo ${TIME_UNIX_TOMORROW}
TIME_UNIX_10days_AGO=$(date -d "$TIME_10days_AGO" +%s)
echo ${TIME_UNIX_10days_AGO}

#每日新增分区
${mysqlCommd} "ALTER TABLE CMPONOLTBaseData ADD PARTITION (PARTITION p${TIME_UNIX_TODAY} VALUES LESS THAN (${TIME_UNIX_TOMORROW}));"
${mysqlCommd} "ALTER TABLE CMPONONUBaseData ADD PARTITION (PARTITION p${TIME_UNIX_TODAY} VALUES LESS THAN (${TIME_UNIX_TOMORROW}));"
${mysqlCommd} "ALTER TABLE CMPONPRTBaseData ADD PARTITION (PARTITION p${TIME_UNIX_TODAY} VALUES LESS THAN (${TIME_UNIX_TOMORROW}));"
${mysqlCommd} "ALTER TABLE ponNetworkResourceTree ADD PARTITION (PARTITION p${TIME_UNIX_TODAY} VALUES LESS THAN (${TIME_UNIX_TOMORROW}));"
${mysqlCommd} "ALTER TABLE oltUpperCoupletRate ADD PARTITION (PARTITION p${TIME_UNIX_TODAY} VALUES LESS THAN (${TIME_UNIX_TOMORROW}));"
${mysqlCommd} "ALTER TABLE onuLowlightAnal ADD PARTITION (PARTITION p${TIME_UNIX_TODAY} VALUES LESS THAN (${TIME_UNIX_TOMORROW}));"

#每日删除旧分区
${mysqlCommd} "ALTER TABLE CMPONOLTBaseData DROP PARTITION p${TIME_UNIX_10days_AGO};"
${mysqlCommd} "ALTER TABLE CMPONONUBaseData DROP PARTITION p${TIME_UNIX_10days_AGO};"
${mysqlCommd} "ALTER TABLE CMPONPRTBaseData DROP PARTITION p${TIME_UNIX_10days_AGO};"
${mysqlCommd} "ALTER TABLE ponNetworkResourceTree DROP PARTITION p${TIME_UNIX_10days_AGO};"
${mysqlCommd} "ALTER TABLE oltUpperCoupletRate DROP PARTITION p${TIME_UNIX_10days_AGO};"
${mysqlCommd} "ALTER TABLE onuLowlightAnal DROP PARTITION p${TIME_UNIX_10days_AGO};"

D2=$(date -d now +%s)
timex=$(($D2-$D1))
echo "脚本处理时间为：${timex}"

