#!/usr/bin/env bash


run_cmd_with_log()
{
    echo `date +"%Y-%m-%d:%H:%M:%S"`
    cmd=$1
    ${cmd}
    echo `date +"%Y-%m-%d:%H:%M:%S"`
}
print_time()
{
    echo `date +"%Y-%m-%d:%H:%M:%S"`
}

info()
{
    echo "[INFO]`date +"%Y-%m-%d:%H:%M:%S"` $1"
}

err()
{
    echo "[ERROR]`date +"%Y-%m-%d:%H:%M:%S"` $1"
}

import_evn()
{
    export HADOOP_HOME=/opt/cloudera/parcels/CDH/lib/hadoop
    export HADOOP_STREAMIMG_JAR="/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar"
    export JAVA_HOME=/usr/local/jdk1.7
    export PATH=/usr/local/python-2.7/bin:${HADOOP_HOME}/bin:${JAVA_HOME}/bin:$PATH
    export GRID_DATA_PATH_BASE="/user/services/push_notification/push_statistics"
    export GRID_DATA_PUSH_STATS="${GRID_DATA_PATH_BASE}/push_stat_hourly"
}

check_if_grid_file_exist()
{
    local grid_file=$1
    hadoop fs -test -e ${grid_file}
    local res=$?
    return ${res}
}

append_string()
{
    local base=$1
    local append=$2
    if [ "$base"x == ""x ]; then
        echo "${append}"
    elif [ "$append"x == ""x ]; then
        echo "$base"
    else
        echo "${base},${append}"
    fi
}

wait_file_ready()
{
    local grid_file=$1
    local timeout=$2
    local passed_time=0
    while(true)
    do
        info "check if file [$grid_file] is ready "
        check_if_grid_file_exist ${grid_file}
        if [ "$?" == "0" ]; then
            info "$grid_file ready"
            break
        fi
        sleep 5s
        if [ "$timeout"x != ""x ]; then
            passed_time=`expr ${passed_time} + 5`
            if [ ${passed_time} -gt ${timeout} ]; then
                return 1
            fi
        fi
    done
    return 0
}


wait_files_ready()
{
    local grid_files=$1
    local timeout=$2
    local passed_time=0
    local file_arr=(${grid_files//,/ })
    local file_arr_length=${#file_arr[@]}
    local ret_code=1
    while(true)
    do
        info "[CHECK] file [$grid_files] is ready "
        local ready_file_num=0
        local ready_files=""
        for file in ${file_arr[@]}
        do
            check_if_grid_file_exist ${file}
            if [ $? == 0 ]; then
                ready_file_num=$((ready_file_num+1))
                ready_files=`append_string "${ready_files}" "${file}"`
			#else
			#	echo "${file} is NOT READY" >&2
            fi

        done
        if [ ${file_arr_length}  -eq ${ready_file_num} ];then
            echo "${ready_files}"
            ret_code=0
            break
        fi
        sleep 5s
        if [ "$timeout"x != ""x ]; then
            passed_time=`expr ${passed_time} + 5`
            if [ ${passed_time} -gt ${timeout} ]; then
                echo "${ready_files}"
                break
            fi
        fi
    done
    return ${ret_code}
}

notify()
{
    local code=$1
    local subject=$2
    local msg=$3
    local to_user=$4
    local status="[FAILED]"
    if [ "x$code" == "x0" ]; then
        status="[SUCCESS]"
    fi
    echo "$msg" | mail -s "$status : ${subject}" ${to_user}

}