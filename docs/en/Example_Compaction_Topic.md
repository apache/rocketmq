# Compaction Topic

## use example

### Turn on the opening of support for orderMessages on namesrv
CompactionTopic relies on orderMessages to ensure consistency
```shell
$ bin/mqadmin updateNamesrvConfig -k orderMessageEnable -v true
```

### create compaction topic
```shell
$ bin/mqadmin updateTopic -w 8 -r 8 -a +cleanup.policy=COMPACTION -n localhost:9876 -t ctopic -o true -c DefaultCluster
create topic to 127.0.0.1:10911 success.
TopicConfig [topicName=ctopic, readQueueNums=8, writeQueueNums=8, perm=RW-, topicFilterType=SINGLE_TAG, topicSysFlag=0, order=false, attributes={+cleanup.policy=COMPACTION}]
```

### produce message
the same with ordinary message
```java
DefaultMQProducer producer = new DefaultMQProducer("CompactionTestGroup");
producer.setNamesrvAddr("localhost:9876");
producer.start();

String topic = "ctopic";
String tag = "tag1";
String key = "key1";
Message msg = new Message(topic, tag, key, "bodys".getBytes(StandardCharsets.UTF_8));
SendResult sendResult = producer.send(msg, (mqs, message, shardingKey) -> {
    int select = Math.abs(shardingKey.hashCode());
    if (select < 0) {
        select = 0;
    }
    return mqs.get(select % mqs.size());
}, key);

System.out.printf("%s%n", sendResult);
```
### consume message
the message offset remains unchanged after compaction. If the consumer specified offset does not exist, return the most recent message after the offset.

In the compaction scenario, most consumption was started from the beginning of the queue.
```java
DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("compactionTestGroup");
consumer.setNamesrvAddr("localhost:9876");
consumer.setPullThreadNums(4);
consumer.start();

Collection<MessageQueue> messageQueueList = consumer.fetchMessageQueues("ctopic");
consumer.assign(messageQueueList);
messageQueueList.forEach(mq -> {
    try {
        consumer.seekToBegin(mq);
    } catch (MQClientException e) {
        e.printStackTrace();
    }
});

Map<String, byte[]> kvStore = Maps.newHashMap();
while (true) {
    List<MessageExt> msgList = consumer.poll(1000);
    if (CollectionUtils.isNotEmpty(msgList)) {
        msgList.forEach(msg -> kvStore.put(msg.getKeys(), msg.getBody()));
    }
}

//use the kvStore
```
