#!/usr/bin/env bash

APP_ACCESS_KEY="5951744265936";
APP_ACCESS_SECRET="r63EXucC6ndd599fvW4mAg==";
BUCKET_NAME="content-resources";
OBJECT_NAME="yidian_user_imeimd5/yidian_user_imeimd5.txt"
out_put="xiaomi.yidian_install."`date +%Y-%m-%d`

java -cp target/galaxy-fds-sdk-java-2.0.0-jar-with-dependencies.jar com.xiaomi.infra.galaxy.fds.services.FDSClient \
$APP_ACCESS_KEY $APP_ACCESS_SECRET $BUCKET_NAME $OBJECT_NAME > $out_put