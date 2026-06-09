## DefaultMQProducer
---
### Class introduction

`public class DefaultMQProducer 
extends ClientConfig 
implements MQProducer`

>`DefaultMQProducer` is the entry point for an application to post messages. Out of the box, you can quickly create a producer with a no-argument constructor. It is mainly responsible for message sending and supports synchronous, asynchronous, and one-way sends. All of these send methods support batch sends. The parameters of the sender can be adjusted through the getter/setter methods provided by this class. `DefaultMQProducer` has multiple send methods, and each method is slightly different. Make sure you know the usage before you use it. Below is a producer example. [See more examples](https://github.com/apache/rocketmq/blob/master/example/src/main/java/org/apache/rocketmq/example/).

``` java 
public class Producer {
    public static void main(String[] args) throws MQClientException {
        // create a producer with producer_group_name
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");

        // start the producer
        producer.start();

        for (int i = 0; i < 128; i++)
            try {
            	// construct the msg
                Message msg = new Message("TopicTest",
                        "TagA",
                        "OrderID188",
                        "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));

                // send sync
                SendResult sendResult = producer.send(msg);

                // print the result
                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
            }

        producer.shutdown();
    }
}
```

**Note** : This class is thread safe. It can be safely shared between multiple threads after configuration and startup is complete.

###  Variable 

|Type|Name| description                                                  |
|------|-------|-------|
|DefaultMQProducerImpl|defaultMQProducerImpl|The producer's internal default implementation|
|String|producerGroup|The producer's group|
|String|createTopicKey| Topics that do not exist on the server are automatically created when the message is sent |
|int|defaultTopicQueueNums|The default number of queues to create a topic|
|int|sendMsgTimeout|The timeout for the message to be sent|
|int|compressMsgBodyOverHowmuch|the threshold of the compress of  message body|
|int|retryTimesWhenSendFailed|Maximum number of internal attempts to send a message in synchronous mode|
|int|retryTimesWhenSendAsyncFailed|Maximum number of internal attempts to send a message in asynchronous mode|
|boolean|retryAnotherBrokerWhenNotStoreOK|Whether to retry another broker if an internal send fails|
|int|maxMessageSize| Maximum length of message body                                   |
|TraceDispatcher|traceDispatcher| Message trackers. Use rpcHook to track messages              |

### construction method 

|Method name|Method description|
|-------|------------|
|DefaultMQProducer()| creates a producer with default parameter values             |
|DefaultMQProducer(final String producerGroup)| creates a producer with producer group name.                 |
|DefaultMQProducer(final String producerGroup, boolean enableMsgTrace)|creates a producer with producer group name and set whether to enable message tracking|
|DefaultMQProducer(final String producerGroup, boolean enableMsgTrace, final String customizedTraceTopic)|creates a producer with producer group name and set whether to enable message tracking、the trace topic.|
|DefaultMQProducer(RPCHook rpcHook)|creates a producer with  a rpc hook.|
|DefaultMQProducer(final String producerGroup, RPCHook rpcHook)|creates a producer with  a rpc hook and producer group.|
|DefaultMQProducer(final String producerGroup, RPCHook rpcHook, boolean enableMsgTrace,final String customizedTraceTopic)|all of above.|

