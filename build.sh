#!/usr/bin/env bash

# BUILD
mvn -f project/pom.xml -Dmaven.test.skip=true package;


# INIT_TEST_ACC
if [ -z $ACC_NAME ]; then
    ACC_NAME=DEMO_TEST_1;
fi
mkdir $ACC_NAME;
cp -r account_template $ACC_NAME;
