# Support Arbitrary-Time-Delay Message
Support Arbitrary-Time-Delay Message

## Design


## Example
```java
DefaultMQProducer producer = new DefaultMQProducer("xxx-producerGroup");
producer.setNamesrvAddr("xxx.xxx.xxx.xxx:xxxx");
producer.start();
 
 
Message message0 = new Message(topic, "delay-msg0".getBytes());
message0.setDelaySecond(3);
producer.send(message0);


Message message1 = new Message(topic, "delay-msg1".getBytes());
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
                System.out.println(messageExt.getStoreTimestamp() - messageExt.getDelayDate().getTime());
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
});
pushConsumer.start();

```
