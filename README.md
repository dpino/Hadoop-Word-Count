Description
-----------

Reads */tmp/wordcount/in/* dir and writes output to 'words'.

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
