# OpenMessaging Example
[OpenMessaging](https://openmessaging.github.io/), which includes the establishment of industry guidelines and messaging, streaming specifications to provide a common framework for finance, e-commerce, IoT and big-data area. The design principles are the cloud-oriented, simplicity, flexibility, and language independent in distributed heterogeneous environments. Conformance to these specifications will make it possible to develop a heterogeneous messaging applications across all major platforms and operating systems.

RocketMQ provides a partial implementation of the OpenMessaging 0.3.1-alpha API (`io.openmessaging:openmessaging-api`), the following examples demonstrate how to access RocketMQ based on OpenMessaging. They mirror the runnable classes in `example/src/main/java/org/apache/rocketmq/example/openmessaging` and expect a nameserver on `localhost:9876` with the environment variable `OMS_RMQ_DIRECT_NAME_SRV=true` set, so the driver talks to the nameserver directly.

## SimpleProducer
The following example shows how to send message to RocketMQ broker in synchronous, asynchronous, or one-way transmissions.

```java
public class SimpleProducer {
    public static final String URL = "oms:rocketmq://localhost:9876/default:default";
    public static final String QUEUE = "OMS_HELLO_TOPIC";

    public static void main(String[] args) {
        final MessagingAccessPoint messagingAccessPoint =
            OMS.getMessagingAccessPoint(URL);

        final Producer producer = messagingAccessPoint.createProducer();

        messagingAccessPoint.startup();
        System.out.printf("MessagingAccessPoint startup OK%n");

        producer.startup();
        System.out.printf("Producer startup OK%n");

        {
            Message message = producer.createBytesMessage(QUEUE, "OMS_HELLO_BODY".getBytes(StandardCharsets.UTF_8));

            SendResult sendResult = producer.send(message);
            System.out.printf("Send sync message OK, msgId: %s%n", sendResult.messageId());
        }

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        {
            final Future<SendResult> result = producer.sendAsync(producer.createBytesMessage(QUEUE,
                "OMS_HELLO_BODY".getBytes(StandardCharsets.UTF_8)));
            result.addListener(future -> {
                if (future.getThrowable() != null) {
                    System.out.printf("Send async message Failed, error: %s%n", future.getThrowable().getMessage());
                } else {
                    System.out.printf("Send async message OK, msgId: %s%n", future.get().messageId());
                }
                countDownLatch.countDown();
            });
        }

        {
            producer.sendOneway(producer.createBytesMessage(QUEUE, "OMS_HELLO_BODY".getBytes(StandardCharsets.UTF_8)));
            System.out.printf("Send oneway message OK%n");
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException ignore) {
        }

        producer.shutdown();
        messagingAccessPoint.shutdown();
    }
}
```

## SimplePullConsumer
Use the OMS PullConsumer to receive messages from a specified queue.

```java
public class SimplePullConsumer {
    public static final String URL = "oms:rocketmq://localhost:9876/default:default";
    public static final String QUEUE = "OMS_CONSUMER";

    public static void main(String[] args) {
        final MessagingAccessPoint messagingAccessPoint =
            OMS.getMessagingAccessPoint(URL);

        messagingAccessPoint.startup();

        final Producer producer = messagingAccessPoint.createProducer();

        final PullConsumer consumer = messagingAccessPoint.createPullConsumer(
            OMS.newKeyValue().put(OMSBuiltinKeys.CONSUMER_ID, QUEUE));

        final String queueName = "TopicTest";

        producer.startup();
        Message msg = producer.createBytesMessage(queueName, "Hello Open Messaging".getBytes(StandardCharsets.UTF_8));
        SendResult sendResult = producer.send(msg);
        System.out.printf("Send Message OK. MsgId: %s%n", sendResult.messageId());
        producer.shutdown();

        consumer.attachQueue(queueName);

        consumer.startup();
        System.out.printf("Consumer startup OK%n");

        // Keep running until we find the one that has just been sent
        boolean stop = false;
        while (!stop) {
            Message message = consumer.receive();
            if (message != null) {
                String msgId = message.sysHeaders().getString(Message.BuiltinKeys.MESSAGE_ID);
                System.out.printf("Received one message: %s%n", msgId);
                consumer.ack(msgId);

                stop = msgId.equalsIgnoreCase(sendResult.messageId());
            } else {
                System.out.printf("Return without any message%n");
            }
        }

        consumer.shutdown();
        messagingAccessPoint.shutdown();
    }
}
```

## SimplePushConsumer
Attaches the OMS PushConsumer to a specified queue and consumes messages by MessageListener.

```java
public class SimplePushConsumer {
    public static final String URL = "oms:rocketmq://localhost:9876/default:default";
    public static final String QUEUE = "OMS_HELLO_TOPIC";

    public static void main(String[] args) {
        final MessagingAccessPoint messagingAccessPoint = OMS
            .getMessagingAccessPoint(URL);

        final PushConsumer consumer = messagingAccessPoint.
            createPushConsumer(OMS.newKeyValue().put(OMSBuiltinKeys.CONSUMER_ID, "OMS_CONSUMER"));

        messagingAccessPoint.startup();
        System.out.printf("MessagingAccessPoint startup OK%n");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.shutdown();
            messagingAccessPoint.shutdown();
        }));

        consumer.attachQueue(QUEUE, (message, context) -> {
            System.out.printf("Received one message: %s%n", message.sysHeaders().getString(Message.BuiltinKeys.MESSAGE_ID));
            context.ack();
        });

        consumer.startup();
        System.out.printf("Consumer startup OK%n");
    }
}
```
