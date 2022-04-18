#!/bin/bash
db=${1:-leveldb}
mvn -pl site.ycsb:$db-binding -am clean package -Dcheckstyle.skip
