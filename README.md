
Flume classes
=============

This project contains utility classes for Flume. You will find :

* The "HeaderAndBodyTextEventSerializer" serializer present in version 1.3 so we could use it in the Cloudera Distribution 4.1.3 (latest version at the time of this writing) which ship Flume 1.2.
* A new "JSONEventSerializer" which write header and body event as JSON lines.

HeaderAndBodyTextEventSerializer
--------------------------------

This class simply writes the header properties and body of the event to the output stream and appends a newline after each event. The "columns" configuration allows to list and order the columns to write. The "format" configuration accept "NATIVE" and "CSV". In the case of the "CSV" serialization, the fields are space delimited and the implementation is extremely simple without any escaping.
 
Example
```coffee
a1.sources.r1.type = syslogtcp
a1.sources.r1.host = 0.0.0.0
a1.sources.r1.port = 5141

a1.sources.r1.interceptors = i1 i2
a1.sources.r1.interceptors.i1.type = timestamp
a1.sources.r1.interceptors.i2.type = host
a1.sources.r1.interceptors.i2.hostHeader = hostname

a1.sinks.s1.type = hdfs
a1.sinks.s1.hdfs.path = hdfs://namenode:8020/user/hdfs/logs.json
a1.sinks.s1.serializer = com.adaltas.flume.serialization.HeaderAndBodyTextEventSerializer$Builder
a1.sinks.s1.serializer.columns = timestamp hostname Facility Severity
a1.sinks.s1.serializer.format = CSV
a1.sinks.s1.serializer.appendNewline = true
```

JSONEventSerializer
-------------------

This class writes the header properties and body of the event as JSON lines. The body is by default associated with the "body" key. The "columns" configuration allows to list and order the columns to write. It must contains the name of the body key if you wish to write the event body. The "body" configuration is the name of the key associated to the event body.

Example
```coffee
a1.sinks.s1.type = hdfs
a1.sinks.s1.hdfs.path = hdfs://namenode:8020/user/hdfs/logs.json
a1.sinks.s1.serializer = com.adaltas.flume.serialization.JSONEventSerializer$Builder
a1.sinks.s1.serializer.columns = timestamp msg
a1.sinks.s1.serializer.body = msg
a1.sinks.s1.serializer.appendNewline = true
```

Download
--------

*   [Version 0.0.1](https://github.com/wdavidw/flume/raw/master/lib/adaltas-flume-0.0.1-SNAPSHOT.jar)

Notes
-----

The JSON serializer as been submitted as [FLUME-1909](https://issues.apache.org/jira/browse/FLUME-1909) to the Flume Jira.

Development
-----------

Maven 3 must be installed on your system. To test and compile the project, run `mvn clean test jar:jar`.

For use with Eclipse, install the (M2E plugin](http://www.eclipse.org/m2e/) and run `mvn eclipse:eclipse`.

Contributors
------------

*	David Worms : <https://github.com/wdavidw>
