#!/usr/bin/env bash

cd ../raft-java-core && mvn clean install -DskipTests
cd -
mvn clean package

EXAMPLE_TAR=raft-java-example-1.9.0-deploy.tar.gz
ROOT_DIR=./env
mkdir -p $ROOT_DIR
cd $ROOT_DIR

mkdir example1
cd example1
cp -f ../../target/$EXAMPLE_TAR .
tar -zxvf $EXAMPLE_TAR
chmod +x ./bin/*.sh
nohup ./bin/run_server.sh ./data "127.0.0.1:8051:1,127.0.0.1:8052:2,127.0.0.1:8053:3" "127.0.0.1:8051:1" &
cd -

mkdir example2
cd example2
cp -f ../../target/$EXAMPLE_TAR .
tar -zxvf $EXAMPLE_TAR
chmod +x ./bin/*.sh
nohup ./bin/run_server.sh ./data "127.0.0.1:8051:1,127.0.0.1:8052:2,127.0.0.1:8053:3" "127.0.0.1:8052:2" &
cd -

mkdir example3
cd example3
cp -f ../../target/$EXAMPLE_TAR .
tar -zxvf $EXAMPLE_TAR
chmod +x ./bin/*.sh
nohup ./bin/run_server.sh ./data "127.0.0.1:8051:1,127.0.0.1:8052:2,127.0.0.1:8053:3" "127.0.0.1:8053:3" &
cd -

mkdir client
cd client
cp -f ../../target/$EXAMPLE_TAR .
tar -zxvf $EXAMPLE_TAR
chmod +x ./bin/*.sh
cd -
