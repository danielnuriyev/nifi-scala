# nifi-scala

## About

This is a example of a Nifi processor written in Scala
with the most essential dependencies in pom.xml

## Prerequisites
jdk 1.8, maven

## Compile
mvn clean install

## Output
target/com-scriptedstuff-nifi-0.0.1-SNAPSHOT.nar

## Deploy
````
/usr/local/nifi-1.0.0/bin/nifi.sh stop
cp target/com-scriptedstuff-nifi-0.0.1-SNAPSHOT.nar /usr/local/nifi-1.0.0/lib/
/usr/local/nifi-1.0.0/bin/nifi.sh start
tail -f /usr/local/nifi-1.0.0/log/nifi-app.log
```
