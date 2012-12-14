Description
-----------

Simple implementation of Word-Count example. 

Input is read from directory */tmp/wordcount/in*, and output is written to */tmp/wordcount/out*.

Compile
-------

$ mvn clean install

Run
---

$ mvn exec:java -Dexec.mainClass=com.igalia.wordcount.App
