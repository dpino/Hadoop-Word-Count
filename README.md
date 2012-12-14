Description
-----------

Reads 'files' table and writes output to 'words'.

Create tables
-------------

INPUT table
-----------

To create table 'files' and load content into it, use: https://github.com/dpino/hbase-loader.git

OUTPUT table
------------

$ hbase shell
> create 'words','number'

Compile
-------

$ mvn clean install

Run
---

$ mvn exec:java -Dexec.mainClass=com.igalia.wordcount.App
