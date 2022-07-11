# Support Arbitrary-Time-Delay Message特性
支持任意时间的延迟消息

## 设计思路
将延迟等级队列当做时间轮，增加一定的延迟等级队列，通过多次投递、组合使用延迟等级队列的方式实现精准的延迟投递
下一版本提供可取消接口

## 使用方法
```java
DefaultMQProducer producer = new DefaultMQProducer("xxx-producerGroup");
producer.setNamesrvAddr("xxx.xxx.xxx.xxx:xxxx");
producer.start();
 
 
Message message0 = new Message(topic, "delay-msg0".getBytes());
// 设置延迟时长, 单位秒
message0.setDelaySecond(3);
producer.send(message0);


Message message1 = new Message(topic, "delay-msg1".getBytes());
// 设置指定延迟时间, 精确到秒
Date delayDate1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("xxxx-xx-xx xx:xx:xx");
message1.setDelayDate(delayDate2);
producer.send(message1);

```

```java
DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer("xxx-consumerGroup");
pushConsumer.setNamesrvAddr("xxx.xxx.xxx.xxx:xxxx");
pushConsumer.subscribe(topic, "*");
pushConsumer.registerMessageListener(new MessageListenerConcurrently() {
    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgList, ConsumeConcurrentlyContext context) {
        for (MessageExt messageExt : msgList) {
            if (messageExt.getDelayDate() != null) {
                // 判断是否达到预期, 真实投递时间与预期时间的误差值
                System.out.println(messageExt.getStoreTimestamp() - messageExt.getDelayDate().getTime());
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
});
pushConsumer.start();

```
