## RocketMQ APIs

This is an independent module which aims to establish a new and unified APIs for Apache RocketMQ, it is almost all about
interfaces, and as far as possible to provide abstract expressions for the various capabilities of RocketMQ.

[RIP-37](https://shimo.im/docs/m5kv92OeRRU8olqX) illustrated about why we draft this module.

## Goal

* Introduce immutability. The behavior of interfaces is more clearly defined, making it easier for newcomers to get
  started, and the behavior of various interfaces is more predictable.
* The new APIs unbinds the implementation and interface to realize real interface-oriented programming, and the upgrade
  cost for users will be lower in the future.

## Example

Firstly, There is a provider based on Java SPI mechanism, provider here can derive specific implementations.

```java
// Find the implementation of APIs according to SPI mechanism.
ClientServiceProvider provider = ClientServiceProvider.loadService();
StaticSessionCredentialsProvider staticSessionCredentialsProvider = new StaticSessionCredentialsProvider(accessKey, secretKey);
ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
    .setAccessPoint(accessPoint)
    .setCredentialProvider(staticSessionCredentialsProvider)
    .build();
```

### Producer
Producer is the only client entity used to publish messages.

```java
Message message = provider.newMessageBuilder()
    .setTopic(topic)
    .setBody(body)
    .setTag(tag)
    .build();
try(Producer producer = provider.newProducerBuilder()
    .setClientConfiguration(clientConfiguration)
    .setTopics(topic)
    .build()) {
    // Message is sent successfully if no exception is thrown.
    SendReceipt sendReceipt = producer.send(message);
}
```

In fact, you can use delay/[FIFO](https://en.wikipedia.org/wiki/FIFO_(computing_and_electronics)) functions by setting
message properties, for this part, you can refer to javadocs. In addition, we also provide transactional semantics for
message publishing.
### Push consumer

Push consumer is a fully managed consumer client that automatically accepts messages from remote sites and caches them
for acceleration. You can simply return the corresponding status in the listener to indicate the consumption result. You
don't need to care about other things in most of the time.

```java
PushConsumer pushConsumer = provider.newPushConsumerBuilder()
    .setClientConfiguration(clientConfiguration)
    .setConsumerGroup(consumerGroup)
    .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
    .setMessageListener(messageView -> {
    // Handle the received message and return the consume result.
    return ConsumeResult.OK;
    })
    .build();
// Release it when you don't need the consumer any more.
pushConsumer.close();
```
### Simple consumer

Simple consumer gives consumers with a simple and efficient API, which requires users to actively obtain messages from
the remote end and perform acknowledge/negative acknowledge. You can assemble most of the features you need on top of
this.

```java
SimpleConsumer simpleConsumer = provider.newSimpleConsumerBuilder()
    .setClientConfiguration(clientConfiguration)
    .setConsumerGroup(consumerGroup)
    .setAwaitDuration(awaitDuration)
    .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
    .build();
final List<MessageView> messageViews = consumer.receive(1, invisibleDuration);
for (MessageView messageView : messageViews) {
    // Or nack them according to your needs.
    simpleConsumer.ack(messageView);
}
```