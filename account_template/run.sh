#!/usr/bin/env bash


# CONFIG
CA_JKS_PASS=;

ACC_TYPE=demo; # live/demo
ACC_USR=user;
ACC_PWD=pass;


mkdir -p logs;
# RUN
nohup java \
 -DUSR=$ACC_USR -DPWD=$ACC_PWD \
 -Djavax.net.ssl.trustStore=../cacerts.jks -Djavax.net.ssl.trustStorePassword=$CA_JKS_PASS \
 -Dcache_dir=../.cache \
 -cp price-deviation.jar strategies.PriceDeviation \
 &> logs/out &

sleep 1;
ls -l logs/;
