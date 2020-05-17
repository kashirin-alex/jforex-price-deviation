# BUILD

mvn -f project/pom.xml -Dmaven.test.skip=true package;


# RUN
DIR=$PWD;
CA_JKS_PASS=123456;
ACC_TYPE=demo; # live/demo
ACC_NAME=####;
ACC_USR=####;
ACC_PWD=####;
nohup java -Dcache_dir=$DIR/$ACC_NAME/.cache -DUSR=$ACC_USR -DPWD=$ACC_PWD -Djavax.net.ssl.trustStore=$DIR/cacerts.jks -Djavax.net.ssl.trustStorePassword=$CA_JKS_PASS -cp project/target/price-deviation-1.0-bundled.jar strategies.PriceDeviation > logs/out &
tail logs/out;
