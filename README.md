#storm-tcp-bolt
##Description

This output bolt receives a byte array and writes it through TCP connection. 

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
|tcp.bolt.host|true|TCP host to send data|
|tcp.bolt.port|true|TCP port to send data|


## Compilation
Use maven
````
mvn clean package
```

