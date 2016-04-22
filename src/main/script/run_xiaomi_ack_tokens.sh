#!/usr/bin/env bash

get_working_dir()
{
    local old_working_dir=`pwd`
    local working_dir=$(cd `dirname $0`; pwd)
    cd ${old_working_dir}
    echo ${working_dir}
}

argv_num=$#

processing_day=`date -d "1 hour ago" +"%Y%m%d"`
processing_hour=`date  -d "1 hour ago" +"%H"`
if [ ${argv_num} -eq 2 ]; then
    processing_day=$1
    processing_hour=$2
fi


# start to run
working_dir=`get_working_dir`
cd ${working_dir}
source ${working_dir}/common.sh
import_evn
print_time

### config starts###
GRID_XIAOMI_ACK_TOKENS_PATH="/user/services/push_notification/xiaomi_ack_tokens"

APP_ACCESS_KEY="5951744265936";
APP_ACCESS_SECRET="r63EXucC6ndd599fvW4mAg==";
BUCKET_NAME="yidianzixun";
OBJECT_NAME="push/push_alias"
out_put="xiaomi.yidian_install.${processing_day}"
### config ends###

# data dir
grid_res_path=${GRID_XIAOMI_ACK_TOKENS_PATH}/${processing_day}
grid_res_path_working=${grid_res_path}.working
local_data_dir="./data/"
local_data="${local_data_dir}/xiaomi_ack_tokens.${processing_day}"
xiaomi_ready_file=${OBJECT_NAME}/_SUCCESS_${processing_day}
#
JAVA_OPTS="-cp .:galaxy-fds-sdk-java-2.0.0-jar-with-dependencies.jar"

# 0. check if already done
hadoop fs -test -e ${grid_res_path}
if [ $? -eq 0 ]; then
    echo "${grid_res_path} is ready, just skip it."
    exit 1
fi

# 1. check if success file is ready
info "check if ready file (${xiaomi_ready_file}) ready"
java ${JAVA_OPTS} com.xiaomi.infra.galaxy.fds.services.FDSClient \
$APP_ACCESS_KEY $APP_ACCESS_SECRET $BUCKET_NAME ${xiaomi_ready_file} > /dev/null

if [ $? -ne 0 ]; then
    echo "${xiaomi_ready_file} is not ready, just quit."
    exit 1
fi

# 2. get the data: msgID\tacktime\talias
mkdir -p ${local_data_dir}
rm ${local_data}
for i in `seq -f "%05g"  0 99`
do
    object=${OBJECT_NAME}/data/part-${i}
    info "get object ${object}"
    java ${JAVA_OPTS} com.xiaomi.infra.galaxy.fds.services.FDSClient \
$APP_ACCESS_KEY $APP_ACCESS_SECRET $BUCKET_NAME ${object} >>${local_data}
    if [ $? -ne 0 ]; then
        echo "could not get object ${object}, quit."
        exit 1
    fi
done

# 3. put the data onto grid
hadoop fs -rm -r -f ${grid_res_path_working}
hadoop fs -mkdir -p ${GRID_XIAOMI_ACK_TOKENS_PATH}
hadoop fs -mkdir -p ${grid_res_path_working}
hadoop fs -put ${local_data} ${grid_res_path_working}
hadoop fs -mv ${grid_res_path_working} ${grid_res_path}
# clean 
rm -rf ${local_data}
