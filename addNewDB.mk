# Add costumed Database to YCSB
## 1. write java class for binding
## 2. add package-info.java to that class
## 3. add module to `pom.xml` at root path
for example: `<module>leveldb</module>`
## 4. add binding to `distribution/pom.xml`
for exampel:

```xml
<dependency>
      <groupId>site.ycsb</groupId>
      <artifactId>leveldb-binding</artifactId>
      <version>${project.version}</version>
</dependency>
```

## 5. add binding to `bin/bindings.properties `
for example: `leveldb:site.ycsb.db.LevelDbClient`