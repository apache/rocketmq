# RocketMQ-LogAppender   [![Build Status](https://travis-ci.org/rocketmq/rocketmq-logappender.svg?branch=master)](https://travis-ci.org/rocketmq/rocketmq-logappender) [![Coverage Status](https://coveralls.io/repos/github/rocketmq/rocketmq-logappender/badge.svg?branch=master)](https://coveralls.io/github/rocketmq/rocketmq-logappender?branch=master)


## Introduction
RocketMQ-LogAppender is a logging component implement of log4j,log4j2 and logback.Taking Apache RocketMQ as broker.
All logs loged will be send to the topic you define.

## Examples

#### log4j example

> config detail

```xml
<appender name="mqAppender1" class="org.apache.rocketmq.logappender.log4j.RocketmqLog4jAppender">
    <param name="Tag" value="log1" />
    <param name="Topic" value="TopicTest" />
    <param name="ProducerGroup" value="log4jxml" />
    <param name="NameServerAddress" value="127.0.0.1:9876"/>
    <layout class="org.apache.log4j.PatternLayout">
        <param name="ConversionPattern" value="%d{yyyy-MM-dd HH:mm:ss}-%p %t %c - %m%n" />
    </layout>
</appender>

<appender name="mqAsyncAppender1" class="org.apache.log4j.AsyncAppender">
    <param name="BufferSize" value="1024" />
    <param name="Blocking" value="false" />
    <appender-ref ref="mqAppender1"></appender-ref>
</appender>

<logger name="testLogger" additivity="false">
    <level value="INFO" />
    <appender-ref ref="mqAsyncAppender1" />
</logger>

```

#### logback example

```xml
<appender name="mqAppender1" class="org.apache.rocketmq.logappender.logback.RocketmqLogbackAppender">
    <tag>log1</tag>
    <topic>TopicTest</topic>
    <producerGroup>logback</producerGroup>
    <nameServerAddress>127.0.0.1:9876</nameServerAddress>
    <layout>
        <pattern>%date %p %t - %m%n</pattern>
    </layout>
</appender>

<appender name="mqAsyncAppender1" class="ch.qos.logback.classic.AsyncAppender">
    <queueSize>1024</queueSize>
    <discardingThreshold>80</discardingThreshold>
    <maxFlushTime>2000</maxFlushTime>
    <neverBlock>true</neverBlock>
    <appender-ref ref="mqAppender1"/>
</appender>
<logger name="testLogger" level="debug" additivity="false">
    <appender-ref ref="mqAsyncAppender1"/>
</logger>

```

More example,like log4j2,please see Test case.
