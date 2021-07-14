# 经常被问到的问题

以下是关于RocketMQ项目的常见问题

## 1 基本

1. **为什么我们要使用RocketMQ而不是选择其他的产品？**

   请参考[为什么要选择RocketMQ](http://rocketmq.apache.org/docs/motivation/)

2. **我是否需要安装其他的软件才能使用RocketMQ，例如zookeeper？**

   不需要，RocketMQ可以独立的运行。

## 2 使用

1. **新创建的Consumer ID从哪里开始消费消息？**

   1）如果发送的消息在三天之内，那么消费者会从服务器中保存的第一条消息开始消费。

   2）如果发送的消息已经超过三天，则消费者会从服务器中的最新消息开始消费，也就是从队列的尾部开始消费。

   3）如果消费者重新启动，那么它会从最后一个消费位置开始消费消息。

2. **当消费失败的时候如何重新消费消息？**

   1）在集群模式下，消费的业务逻辑代码会返回Action.ReconsumerLater，NULL，或者抛出异常，如果一条消息消费失败，最多会重试16次，之后该消息会被丢弃。

   2）在广播消费模式下，广播消费仍然保证消息至少被消费一次，但不提供重发的选项。

3. **当消费失败的时候如何找到失败的消息？**

   1）使用按时间的主题查询，可以查询到一段时间内的消息。

   2）使用主题和消息ID来准确查询消息。

   3）使用主题和消息的Key来准确查询所有消息Key相同的消息。

4. **消息只会被传递一次吗？**

   RocketMQ 确保所有消息至少传递一次。 在大多数情况下，消息不会重复。

5. **如何增加一个新的Broker？**

   1）启动一个新的Broker并将其注册到name server中的Broker列表里。

   2）默认只自动创建内部系统topic和consumer group。 如果您希望在新节点上拥有您的业务主题和消费者组，请从现有的Broker中复制它们。 我们提供了管理工具和命令行来处理此问题。

## 3 配置相关

以下回答均为默认值，可通过配置修改。

1. **消息在服务器上可以保存多长时间？**

   存储的消息将最多保存 3 天，超过 3 天未使用的消息将被删除。

2. **消息体的大小限制是多少？**

   通常是256KB

3. **怎么设置消费者线程数？**

   当你启动消费者的时候，可以设置 ConsumeThreadNums属性的值，举例如下：

   ```java
   consumer.setConsumeThreadMin(20);
   consumer.setConsumeThreadMax(20);
   ```

## 4 错误

1. **当你启动一个生产者或消费者的过程失败了并且错误信息是生产者组或消费者重复**

   原因：使用同一个Producer/Consumer Group在同一个JVM中启动多个Producer/Consumer实例可能会导致客户端无法启动。

   解决方案：确保一个 Producer/Consumer Group 对应的 JVM 只启动一个 Producer/Consumer 实例。

2. **消费者无法在广播模式下开始加载 json 文件**

   原因：fastjson 版本太低，无法让广播消费者加载本地 offsets.json，导致消费者启动失败。 损坏的 fastjson 文件也会导致同样的问题。

   解决方案：Fastjson 版本必须升级到 RocketMQ 客户端依赖版本，以确保可以加载本地 offsets.json。 默认情况下，offsets.json 文件在 /home/{user}/.rocketmq_offsets 中。 或者检查fastjson的完整性。

3. **Broker崩溃以后有什么影响？**

   1）Master节点崩溃

   消息不能再发送到该Broker集群，但是如果您有另一个可用的Broker集群，那么在主题存在的条件下仍然可以发送消息。消息仍然可以从Slave节点消费。

   2）一些Slave节点崩溃

   只要有另一个工作的slave，就不会影响发送消息。 对消费消息也不会产生影响，除非消费者组设置为优先从该Slave消费。 默认情况下，消费者组从 master 消费。

   3）所有Slave节点崩溃

   向master发送消息不会有任何影响，但是，如果master是SYNC_MASTER，producer会得到一个SLAVE_NOT_AVAILABLE，表示消息没有发送给任何slave。 对消费消息也没有影响，除非消费者组设置为优先从slave消费。 默认情况下，消费者组从master消费。

4. **Producer提示“No Topic Route Info”，如何诊断？**

   当您尝试将消息发送到一个路由信息对生产者不可用的主题时，就会发生这种情况。

   1）确保生产者可以连接到名称服务器并且能够从中获取路由元信息。

   2）确保名称服务器确实包含主题的路由元信息。 您可以使用管理工具或 Web 控制台通过 topicRoute 从名称服务器查询路由元信息。

   3）确保您的Broker将心跳发送到您的生产者正在连接的同一name server列表。

   4）确保主题的权限为6(rw-)，或至少为2(-w-)。

   如果找不到此主题，请通过管理工具命令updateTopic或Web控制台在Broker上创建它。