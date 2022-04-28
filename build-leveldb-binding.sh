#!/bin/bash
db=${1:-leveldbjni}
mvn -pl site.ycsb:$db-binding -am clean package -Dcheckstyle.skip
[ $? -eq 0 ] && cp -f \
/home/lg/lsm/YCSB/leveldbjni/target/leveldbjni-binding-0.18.0-SNAPSHOT.jar \
/home/lg/lsm/YCSB-leveldbjni-binding/lib/
