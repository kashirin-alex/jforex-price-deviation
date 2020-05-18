#!/usr/bin/env bash

# BUILD
mvn -f project/pom.xml -Dmaven.test.skip=true package;



# MOVE CLASS TO ACC

if [ -z $ACC_NAME ]; then
    # INIT_TEST_ACC
    ACC_NAME=DEMO_TEST_1;
fi

if [ ! -d accounts/$ACC_NAME ]; then
    mkdir -p accounts/$ACC_NAME;
    cp -r account_template/* accounts/$ACC_NAME/;
fi

cp project/target/price-deviation-1.0-bundled.jar accounts/$ACC_NAME/price-deviation.jar;

cd accounts/$ACC_NAME/;
