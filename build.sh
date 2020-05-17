#!/usr/bin/env bash

# BUILD
mvn -f project/pom.xml -Dmaven.test.skip=true package;



# MOVE CLASS TO ACC

if [ -z $ACC_NAME ]; then
    # INIT_TEST_ACC
    ACC_NAME=DEMO_TEST_1;
fi

mkdir $ACC_NAME;
cp -r account_template/* $ACC_NAME;
cp project/target/price-deviation-1.0-bundled.jar $ACC_NAME/price-deviation.jar;

