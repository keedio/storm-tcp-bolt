#storm-tcp-bolt
##Description

This bolt receives a byte array writes the bytes to the configured host and port. It is used in order to inject data to Splunk.


## Property file configuration
```
...
# Splunk tcp properties
tcp.bolt.host=tcpHost
tcp.bolt.port=2000
...
```

|property|mandatory|description
|--------|------------|-------------|
|tcp.bolt.host|true|Splunk host|
|tcp.bolt.port|true|Splunk port|


## Compilation
Use maven
````
mvn clean package
```

