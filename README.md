
Flume classes
=============

This project contains utility classes for Flume.

At the moment, we have backported the "HeaderAndBodyTextEventSerializer" serializer present in version 1.3 so we could use it in the Cloudera Distribution 4.1.3 (latest version at the time of this writing) which ship Flume 1.2.

Note, Maven 3 must be installed on your system.

Download
--------

*   [Version 0.0.1](https://github.com/wdavidw/flume/blob/master/lib/adaltas-flume-0.0.1-SNAPSHOT.jar)

Build
-----

```bash
mvn jar:jar
```

Eclipse
-------

Install the (M2E plugin](http://www.eclipse.org/m2e/).

```bash
mvn eclipse:eclipse
```

Contributors
------------

*	David Worms : <https://github.com/wdavidw>
