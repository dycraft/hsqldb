# hsqldb

## 如何测试

[HSQLDB 官方测试地址](http://hsqldb.org/web/hsqlPerformanceTests.html)

### TPC-B

位于工程根目录

#### 1. sheer speed of the multithreaded engine

- memory tables
- no logging

```java
java -server -Xmx1536M out/production/org.hsqldb.test.TestBench -tps 40 -driver out/production/org.hsqldb.jdbcDriver -url jdbc:hsqldb:mem:test;hsqldb.tx=mvcc -user sa -init -clients 4 -tpc 8000
```

#### 2. automatic checkpoints (memory)

- memory tables
- logs the statement

```java
java -server -Xmx1536M out/production/org.hsqldb.test.TestBench -tps 40 -driver out/production/org.hsqldb.jdbcDriver -url jdbc:hsqldb:file:test;hsqldb.log_size=200;hsqldb.tx=mvcc -user sa -init -clients 4 -tpc 8000
```

#### 3. automatic checkpoints (cached)

- cached tables
- logs the statement

```java
java -server -Xmx128M out/productionorg.hsqldb.test.TestBench -tps 40 -driver out/productionorg.hsqldb.jdbcDriver -url jdbc:hsqldb:file:test;hsqldb.default_table_type=cached;hsqldb.log_size=200;hsqldb.tx=mvcc -user sa -init -clients 4 -tpc 8000
```

