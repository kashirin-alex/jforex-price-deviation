#!/usr/bin/env bash


# CONFIG
DIR=$PWD;
CA_JKS_PASS=;

ACC_TYPE=demo; # live/demo
ACC_USR=;
ACC_PWD=;

# RUN
nohup java -Dcache_dir=$DIR/.cache -DUSR=$ACC_USR -DPWD=$ACC_PWD -Djavax.net.ssl.trustStore=$DIR/../cacerts.jks -Djavax.net.ssl.trustStorePassword=$CA_JKS_PASS -cp $DIR/../project/target/price-deviation-1.0-bundled.jar strategies.PriceDeviation &> logs/out &
tail logs/out;
