API Reference Python
====================

[librocketmqclientpython](https://github.com/apache/rocketmq-client-python) 模块的 API  
注：对应的 rocketmq-client-python 版本为 1.2.0

状态(Status) API
----------------
- ### ```class CConsumeStatus```
    父类: ```Boost.Python.enum```

    - #### ```E_CONSUME_SUCCESS```
        值为 0，消费成功

    - #### ```E_RECONSUME_LATER```
        值为 1，延迟消费

- ### ```class CSendStatus```
    父类: ```Boost.Python.enum```

    - #### ```E_SEND_OK```
        值为 0，发送成功

    - #### ```E_SEND_FLUSH_DISK_TIMEOUT```
        值为 1，写入磁盘超时

    - #### ```E_SEND_FLUSH_SLAVE_TIMEOUT```
        值为 2，写入从节点超时

    - #### ```E_SEND_SLAVE_NOT_AVAILABLE ```
        值为 3，从节点不可用

- ### ```class CStatus```
    父类: ```Boost.Python.enum```

    - #### ```OK```
        值为 0，成功
        
    - #### ```NULL_POINTER```
        值为 1，失败
        
消息(Message) API
-----------------
注意：生产者产生的消息和消费者消费的消息是不同的类型，对应的 API 不应混用

- ### ```def CreateMessage(topic)```
    参数：
    - topic：字符串类型，队列名
    
    返回值：消息实例
    
    创建消息实例，并设置了 topic 字段  
    注意使用完需要销毁实例

- ### ```def SetByteMessageBody(message, body, bytesLength)```
    参数：
    - message：消息实例
    - body：字节类型消息体
    - bytesLength：字节长度

    设置消息体

- ### ```def SetMessageBody(message, body)```
    参数：
    - message：消息实例
    - body：字符串类型消息体

    设置消息体  
    ```python
    message = CreateMessage("test")
    SetMessageBody(message, json.dumps({"test": True, "data": {"name": "activemq", "sub": [2, 3, 4]}, "id": 123}))
    ```

- ### ```def SetMessageKeys(message, key)```
    参数：
    - message：消息实例
    - key：字符串 key

    设置消息的 key，重复设置会发生覆盖

- ### ```def SetDelayTimeLevel(message, level)```
    参数：
    - message：消息实例
    - level：整型，延迟等级，0 表示不延迟，从 1 开始，分别为延迟 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h

    设置消息延迟等级

- ### ```def SetMessageProperty(message, key, value)```
    参数：
    - message：消息实例
    - key：字符串类型，属性
    - value：字符串类型，属性值
    
    设置消息 key 的值

- ### ```def SetMessageTags(mesage, tag)```
    参数：
    - message：消息实例
    - tag：设置消息标签

    设置消息 tag，重复设置会覆盖

- ### ```def SetMessageTopic(message, topic)```
    参数：
    - message：消息实例
    - topic：设置消息要发送到的 topic

    设置消息 topic

- ### ```def DestroyMessage(message)```
    参数：
    - message：消息实例
    
    销毁消息实例

---

- ### ```def GetMessageBody(message)```
    参数：
    - message：消息实例 
    
    返回值：消息体

    提取消息体

- ### ```def GetMessageId(message)```
    参数：
    - message：消息实例 
    
    返回值：消息 ID

    提取消息 ID
    ```python
    print(GetMessageId(message))  # 10F1A8C000005DEB000065A75A120100
    ```

- ### ```def GetMessageKeys(message)```
    参数：
    - message：消息实例 
    
    返回值：消息的 key

    提取消息的 key

- ### ```def GetMessageProperty(message, key)```
    参数：
    - message：消息实例 
    - key：字符串类型，属性
    
    返回值：消息的属性值

    提取消息中指定属性的值

- ### ```def GetMessageTags(message)```
    参数：
    - message：消息实例 
    
    返回值：消息标签

    提取消息的标签

- ### ```def GetMessageTopic(message)```
    参数：
    - message：消息实例 
    
    返回值：消息的 topic

    提取消息的 topic

生产者(Producer) API
--------------------
- ### ```def CreateProducer(name)```
    参数：
    - name：生产者名称 
    
    返回值：生产者

    创建生产者

- ### ```def SetProducerNameServerAddress(producer, namesrv)```
    参数：
    - producer：生产者
    - namesrv：Name server 地址

    设置生产者 name server 地址  
    ```python
    SetProducerNameServerAddress(producer, "192.168.1.100:9876;192.168.1.101:9876;192.168.1.102:9876;192.168.1.103:9876")
    ```

- ### ```def SetProducerNameServerDomain(producer, domain)```
    参数：
    - producer：生产者
    - domain：Name server 域名
    
    设置生产者 name server 域名

- ### ```def SetProducerInstanceName(producer, name)```
    参数：
    - producer：生产者
    - name：生产者名称

    设置生产者名称

- ### ```def SetProducerSessionCredentials(producer, accessKey, secretKey, channel)```
    参数：
    - producer：生产者
    - accessKey：密钥 access key
    - secretKey：密钥 secret key
    - channel：认证渠道

    设置密钥

- ### ```def StartProducer(producer)```
    参数：
    - producer：生产者
    
    启动生产者，需要在发送消息前启动

- ### ```def SendMessageOneway(producer, message)```
    参数：
    - producer：生产者
    - message：消息实例

    单程发送消息

- ### ```def SendMessageOrderly(producer, message, autoRetryTimes, args, queueSelectorCallback)```
    参数：
    - producer：生产者
    - message：消息实例
    - autoRetryTimes：自动重试次数
    - args：回调参数
    - callback：用于选择要发送的队列，返回值为队列序号，比如返回 0

    顺序发送消息

- ### ```def SendMessageSync(producer, message)```
    参数：
    - producer：生产者
    - message：消息实例

    同步发送消息

- ### ```def ShutdownProducer(producer)```
    参数：
    - producer：生产者

    关闭生产者

- ### ```def DestroyProducer(producer)```
    参数：
    - producer：生产者

    销毁生产者实例

- ### ```class SendResult```
    父类: ```Boost.Python.instance```  
    同步发送消息的返回值类型

    参数：
    - #### ```sendStatus```
        发送结果的状态

    - #### ```offset```
        发送的消息在 broker 中的偏移量
    
    - #### ```def GetMsgId```
        获取发送结果的 id


消费者(Consumer) API
--------------------
完整的 Push Consumer API

- ### ```def CreatePushConsumer(group)```
    参数：
    - group：消费组名称
    
    返回值：消费者
    
    创建消费者

- ### ```def SetPushConsumerInstanceName(consumer, name)```
    参数：
    - consumer：消费者
    - name：消费者名称
    
    设置消费者名称

- ### ```def SetPushConsumerNameServerAddress(consumer, namesrv)```
    参数：
    - consumer：消费者
    - namesrv：Name server 地址

    设置消费者 name server 地址

- ### ```def SetPushConsumerNameServerDomain(consumer, domain)```
    参数：
    - consumer：消费者
    - domsin：Name server 域名

    设置消费者 name server 域名

- ### ```def SetPushConsumerMessageBatchMaxSize(consumer, size)```
    参数：
    - consumer：消费者
    - size：批量消费最大个数

    设置消费者批量大小

- ### ```def SetPushConsumerSessionCredentials(consumer, accessKey, secretKey,channel)```
    参数：
    - consumer: 消费者
    - accessKey：密钥 access key
    - secretKey：密钥 secret key
    - channel：认证渠道

    设置密钥

- ### ```def SetPushConsumerThreadCount(consumer, count)```
    参数：
    - consumer: 消费者
    - count: 线程个数

    设置消费者的线程数量

- ### ```def Subscribe(consumer, topic, tagPattern)```
    参数：
    - consumer: 消费者
    - topic: 要消费的 topic
    - tagPattern：要消费的消息标签通配符

    消费者订阅 topic 中 tag 满足通配符 tagPattern 的数据  
    ```python
    Subscribe(consumer, "test", "*")  # 消费 test 中的所有数据
    ```

- ### ```def RegisterMessageCallback(consumer, callback, args)```
    参数：
    - consumer: 消费者
    - callback: 收到消息时的回调函数，注意需要返回值
    - args：回调函数参数

    注册消费消息时的回调函数  
    ```python
    def printData(message, args):
        print(GetMessageId(message))
        return 0

    RegisterMessageCallback(consumer, printData, None)
    ```

- ### ```def StartPushConsumer(consumer)```
    参数：
    - consumer: 消费者

    启动消费者

- ### ```def ShutdownPushConsumer(consumer)```
    参数：
    - consumer: 消费者

    关闭消费者

- ### ```def DestroyPushConsumer(consumer)```
    参数：
    - consumer: 消费者

    销毁消费者

其他
----
- ### ```def GetVersion```
    获取版本信息  
    当前版本为 ```PYTHON_CLIENT_VERSION: 1.2.0, BUILD DATE: 04-12-2018```
