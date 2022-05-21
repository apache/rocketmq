## RocketMQ Thin Client

This part is a lightweight implementation of the apis module. Add dependency to your `pom.xml`, and
replace `${rocketmq.version}` by the latest version.

```xml

<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-thin-client</artifactId>
    <version>${rocketmq.version}</version>
</dependency>
```

Then you can publish or receive messages according to the [instructions of the apis module](../apis/README.md).
