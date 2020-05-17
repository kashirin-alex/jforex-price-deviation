#!/usr/bin/env bash


# CONFIG
CA_JKS_PASS=;

ACC_TYPE=demo; # live/demo
ACC_USR=;
ACC_PWD=;


# RUN
nohup java -Dcache_dir=.cache -DUSR=$ACC_USR -DPWD=$ACC_PWD -Djavax.net.ssl.trustStore=../cacerts.jks -Djavax.net.ssl.trustStorePassword=$CA_JKS_PASS -cp price-deviation.jar strategies.PriceDeviation &> logs/out &

sleep 1;
tail logs/out;
