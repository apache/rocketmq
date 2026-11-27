# Light message queue (LMQ)
LMQ采用的读放大的策略，写一份数据，多个LMQ队列分发，
因为存储的成本和效率对用户的体感最明显。写多份不仅加大了存储成本，同时也对性能和数据准确一致性提出了挑战。

![](image/LMQ_1.png)

上图描述的是LMQ的队列存储模型，消息可以来自各个接入场景
（如服务端的MQ/AMQP，客户端的MQTT），但只会写一份存到commitlog里面，然后分发出多个需求场景的队列索引（ConsumerQueue），如服务端场景（MQ/AMQP）可以按照一级Topic队列进行传统的服务端消费，客户端MQTT场景可以按照MQTT多级Topic(也即 LMQ)进行消费消息。

## 一、broker启动配置


broker.conf文件需要增加以下的配置项，开启LMQ开关，这样就可以识别LMQ相关属性的消息，进行原子分发消息到LMQ队列
```properties
enableLmq = true
enableMultiDispatch = true
```
## 二、发送消息
发送消息的时候通过设置 INNER_MULTI_DISPATCH 属性，LMQ queue使用逗号分割，queue前缀必须是 %LMQ%，这样broker就可以识别LMQ queue.
以下代码只是demo伪代码 具体逻辑参照执行即可
```java
DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
producer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
producer.start();


/*
* Create a message instance, specifying topic, tag and message body.
*/
Message msg = new Message("TopicTest" /* Topic */,
                          "TagA" /* Tag */,
                          ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                         );
/*
* INNER_MULTI_DISPATCH property and PREFIX must start as "%LMQ%",
* If it is multiple LMQ, need to use “,” split
*/
message.putUserProperty("INNER_MULTI_DISPATCH", "%LMQ%123,%LMQ%456");
/*
* Call send message to deliver message to one of brokers.
*/
SendResult sendResult = producer.send(msg);
```
## 三、拉取消息
LMQ queue在每个broker上只有一个queue，也即queueId为0， 指明轻量级的MessageQueue，就可以拉取消息进行消费。
以下代码只是demo伪代码 具体逻辑参照执行即可
```java
DefaultMQPullConsumer defaultMQPullConsumer = new DefaultMQPullConsumer();
defaultMQPullConsumer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
defaultMQPullConsumer.setVipChannelEnabled(false);
defaultMQPullConsumer.setConsumerGroup("CID_RMQ_SYS_LMQ_TEST");
defaultMQPullConsumer.setInstanceName("CID_RMQ_SYS_LMQ_TEST");
defaultMQPullConsumer.setRegisterTopics(new HashSet<>(Arrays.asList("TopicTest")));
defaultMQPullConsumer.setBrokerSuspendMaxTimeMillis(2000);
defaultMQPullConsumer.setConsumerTimeoutMillisWhenSuspend(3000);
defaultMQPullConsumer.start();

String brokerName = "set broker Name";
MessageQueue mq = new MessageQueue("%LMQ%123", brokerName, 0);
defaultMQPullConsumer.getDefaultMQPullConsumerImpl().getRebalanceImpl().getmQClientFactory().updateTopicRouteInfoFromNameServer("TopicTest");

Thread.sleep(30000);
Long offset = defaultMQPullConsumer.maxOffset(mq);

defaultMQPullConsumer.pullBlockIfNotFound(
                mq, "*", offset, 32,
                new PullCallback() {
                    @Override
                    public void onSuccess(PullResult pullResult) {
                        List<MessageExt> list = pullResult.getMsgFoundList();
                        if (list == null || list.isEmpty()) {
                            return;
                        }
                        for (MessageExt messageExt : list) {
                            System.out.println(messageExt);
                        }    
                    }
                    @Override
                    public void onException(Throwable e) {
                       
                    }
});
```

## 四、查看轻量主题列表

`mqadmin getLiteTopicList`（别名 `listLiteTopic`）可以按父 Topic 枚举当前 broker 上已经写入消息的轻量主题，并展示每个轻量主题的位点、最后更新时间、订阅者数量和分片归属。命令支持通过 `-x/--prefix` 按轻量主题名前缀过滤，并通过 `-m/--maxCount` 限制单个 broker 返回的条目数；broker 会将 `maxCount` 限制在内部允许的最大值内。

```bash
sh bin/mqadmin getLiteTopicList -n 127.0.0.1:9876 -p TopicTest
```

示例输出如下：

```text
Lite Topic List: [TopicTest]
#Broker Name                   #LiteTopic                               #MinOffset     #MaxOffset     #LastUpdate               #Sharding     #SubCount
broker-a                       order-1                                  0              128            2026-10-18 10:20:30       true          2
broker-a                       order-2                                  5              64             2026-10-18 10:18:12       true          0
```

使用前缀过滤并限制返回数量：

```bash
sh bin/mqadmin getLiteTopicList -n 127.0.0.1:9876 -p TopicTest -x order- -m 20
```

输出中的 `#MinOffset` 和 `#MaxOffset` 对应轻量主题底层 LMQ 的消息位点范围，`#LastUpdate` 是最后一条消息的存储时间，`#Sharding` 表示当前 LMQ 是否归属于该 broker，`#SubCount` 是订阅该轻量主题的客户端数量。如果某个 broker 返回的结果被 `maxCount` 截断，命令会额外打印 `increase -m to list more` 提示。
​

