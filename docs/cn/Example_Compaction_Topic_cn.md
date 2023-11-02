# Compaction Topic

## 使用方式

### 打开namesrv上支持顺序消息的开关
CompactionTopic依赖顺序消息来保障一致性
```shell
$ bin/mqadmin updateNamesrvConfig -k orderMessageEnable -v true
```

### 创建compaction topic

```shell
$ bin/mqadmin updateTopic -w 8 -r 8 -a +cleanup.policy=COMPACTION -n localhost:9876 -t ctopic -o true -c DefaultCluster
create topic to 127.0.0.1:10911 success.
TopicConfig [topicName=ctopic, readQueueNums=8, writeQueueNums=8, perm=RW-, topicFilterType=SINGLE_TAG, topicSysFlag=0, order=false, attributes={+cleanup.policy=COMPACTION}]
```

### 生产数据

与普通消息一样

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

### 消费数据

消费offset与compaction之前保持不变，如果指定offset消费，当指定的offset不存在时，返回后面最近的一条数据
在compaction场景下，大部分消费都是从0开始消费完整的数据

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