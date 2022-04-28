
# leveldbjni: javaJNI interface for leveldb-like database to integrate into ycsb.

Note: you must install leveldb share library and the db_wrapper first. and make sure java can find the library.

yscb porperties:
- `leveldb.dir`: the directory of the storage of db (no default)
- `leveldb.type`: 0|1, 0 for original leveldb, 1 for modified hotdb (default 0)
- `levedb.optdir`: the directroy of the configuration file, both leveldb and hotdb must store in this dir, named leveldb.ini and hotdb respectively. (default /home/lg/CLionProjects/hotness_aware/config)

To open shell for interactive debugging (with default leveldb):

```bash
./bin/ycsb.sh shell leveldbjni -p leveldb.dir=/somepath/ -p leveldb.optdir=/anotherpath/
```
