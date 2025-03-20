## Client Configuration

Relative to RocketMQ's Broker cluster, producers and consumers are clients. This section describes the common behavior configuration of producers and consumers, including updates for **RocketMQ 5.x**.

### 1 Client Addressing Mode

`RocketMQ` allows clients to locate the `Name Server`, which then helps them find the `Broker`. Below are various configurations, prioritized from highest to lowest. Higher priority configurations override lower ones.

- Specified `Name Server` address in the code (multiple addresses separated by semicolons):

```java
producer.setNamesrvAddr("192.168.0.1:9876;192.168.0.2:9876");  
consumer.setNamesrvAddr("192.168.0.1:9876;192.168.0.2:9876");
```

- Specified `Name Server` address in Java setup parameters:

```text
-Drocketmq.namesrv.addr=192.168.0.1:9876;192.168.0.2:9876  
```

- Specified `Name Server` address in environment variables:

```text
export NAMESRV_ADDR=192.168.0.1:9876;192.168.0.2:9876   
```

- HTTP static server addressing (default):

Clients retrieve `Name Server` addresses from a static HTTP server:
```text
http://jmenv.tbsite.net:8080/rocketmq/nsaddr
```

- **New in RocketMQ 5.x:**
  - Improved service discovery mechanism, allowing dynamic Name Server registration.
  - Introduces `VIP_CHANNEL_ENABLED` for better failover:

  ```java
  producer.setVipChannelEnabled(false);
  consumer.setVipChannelEnabled(false);
  ```

### 2 Client Configuration

`DefaultMQProducer`, `TransactionMQProducer`, `DefaultMQPushConsumer`, and `DefaultMQPullConsumer` all extend the `ClientConfig` class, which provides common client configurations.

#### 2.1 Client Common Configuration

| Parameter Name                | Default Value  | Description  |
|-------------------------------|---------------|--------------|
| namesrvAddr                   |               | Name Server address list (semicolon-separated) |
| clientIP                      | Local IP      | Client's local IP address (useful if automatic detection fails) |
| instanceName                  | DEFAULT       | Unique name for the client instance |
| clientCallbackExecutorThreads | 4             | Number of communication layer asynchronous callback threads |
| pollNameServerInterval        | 30000         | Interval (ms) to poll Name Server |
| heartbeatBrokerInterval       | 30000         | Interval (ms) for sending heartbeats to Broker |
| persistConsumerOffsetInterval | 5000          | Interval (ms) for persisting consumer offsets |
| **autoUpdateNameServer** (5.x) | true          | Automatically update Name Server addresses from registry |
| **instanceId** (5.x)          |               | Unique identifier for each client instance |

#### 2.2 Producer Configuration

| Parameter Name                 | Default Value         | Description  |
|--------------------------------|----------------------|--------------|
| producerGroup                  | DEFAULT_PRODUCER     | Producer group name |
| sendMsgTimeout                 | 3000                 | Timeout (ms) for sending messages |
| retryTimesWhenSendFailed       | 2                    | Max retries for failed messages |
| **enableBatchSend** (5.x)      | true                 | Enables batch message sending |
| **enableBackPressure** (5.x)   | true                 | Prevents overload in high-traffic scenarios |

#### 2.3 PushConsumer Configuration

| Parameter Name                      | Default Value                      | Description |
|--------------------------------------|------------------------------------|-------------|
| consumerGroup                        | DEFAULT_CONSUMER                  | Consumer group name |
| messageModel                         | CLUSTERING                        | Message consumption mode (CLUSTERING or BROADCAST) |
| consumeFromWhere                     | CONSUME_FROM_LAST_OFFSET          | Default consumption position |
| consumeThreadMin                     | 20                                | Min consumption thread count |
| consumeThreadMax                     | 20                                | Max consumption thread count |
| **Rebalance Strategies (5.x)**        | AllocateMessageQueueAveragelyByCircle | New rebalance strategy for better distribution |

#### 2.4 PullConsumer Configuration

| Parameter Name                     | Default Value                 | Description |
|------------------------------------|------------------------------|-------------|
| consumerGroup                      | DEFAULT_CONSUMER             | Consumer group name |
| brokerSuspendMaxTimeMillis         | 20000                        | Max suspension time (ms) for long polling |
| consumerPullTimeoutMillis          | 10000                        | Timeout (ms) for pull requests |
| **messageGroup** (5.x)             |                              | Enables orderly consumption based on groups |

#### 2.5 Message Data Structure

| Field Name        | Default Value  | Description |
|-------------------|---------------|-------------|
| Topic            | null          | Required: Name of the message topic |
| Body             | null          | Required: Message content |
| Tags             | null          | Optional: Tag for filtering |
| Keys             | null          | Optional: Business keys (e.g., order IDs) |
| Flag             | 0             | Optional: Custom flag |
| DelayTimeLevel   | 0             | Optional: Message delay level |
| WaitStoreMsgOK   | TRUE          | Optional: Acknowledgment before storing |
| **maxReconsumeTimes** (5.x) |   | Max retries before moving to dead-letter queue |
| **messageGroup** (5.x) |   | Group-based message ordering |

---

