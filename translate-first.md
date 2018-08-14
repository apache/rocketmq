# 为什么要做RocketMQ

在早期阶段，我们基于ActiveMQ 5.x（5.3之前的版本）构建了分布式消息传递中间件。我们的业务将其用于异步通信，搜索，社交网络活动流，数据管道，甚至在阿里巴巴的交易流程中。随着我们的交易业务吞吐量的增加，来自于我们消息集群的压力也变得紧急。


# 为什么选择RocketMQ
基于我们的研究，随着队列和虚拟主题的使用量不断增加，ActiveMQ IO模块达到了性能的瓶颈。我们尽力通过节流，断路器或降级解决这个问题，但效果不佳。因此，在这个时候我们开始关注流行的Kafka信息系统解决方案。不幸的是，Kafka不能达到我们的业务需求，特别是在低延迟和高可靠性方面，[详见此处](/rocketmq/how-to-support-more-queues-in-rocketmq/)。

在这种情况下，我们决定发明一个信息传递引擎来解决更广泛的使用场景，包括从传统的发布/子场景到大容量的实时零容量容错事务系统。我们相信这个解决方案会对大家有帮助，因此想在社区开源。今天，超过100个公司在他们的业务中使用RocketMQ的开源版本。我们也发布了一个基于RocketMQ的商用发行版，这是一个PaaS产品，名为阿里巴巴云平台。

下面的表格阐述了RocketMQ，ActiveMQ，Kafka（根据awesome-java排名，Apache最流行的消息传递解决方案）的差异：


请注意，这个文档是由RocketMQ团队撰写。尽管这个文档是希望对技术和特征进行公平的比较，但是作者的专业知识和偏见明显偏爱于RocketMQ.

下表是一个方便的快速参考，可以一目了然地发现RocketMQ及其最受欢迎的替代品之间的差异。


|消息传递产品| 客户端 SDK |协议和规范|订购信息|预定消息|批量消息|广播消息	|消息过滤|服务器触发重新投递|消息存储|消息溯源|消息优先级|高可用性和故障转移|消息跟踪|配置|管理和操作工具|
| ---------- | --- | --- |--- |--- | ---------- | --- | --- |--- |--- | ---------- | --- | --- |--- |--- |--- |
|ActiveMQ	|Java, .NET, C++ etc.|	Push model, 支持 OpenWire, STOMP, AMQP, MQTT, JMS|独家消费者或独家队列可以确保订购|	支持	|不支持|	支持|支持|不支持|使用JDBC和高性能日志支持非常快速的持久性,例如levelDB, kahaDB|支持|支持|支持, 根据存储，如果使用kahadb，则需要ZooKeeper服务器	|不支持|默认配置是低级别的，用户需要优化配置参数|支持|
| Kafka | Java, Scala etc. | Pull model, 支持 TCP |确保在分区内对消息进行排序|不支持 | 与异步生产者支持 | 不支持 | 支持, 可以使用Kafka Streams来过滤邮件|不支持 |高性能文件存储| 支持偏移表示| 不支持 | 支持, 需要一个ZooKeeper服务器 |不支持 |Kafka使用键值对格式来配置，可以从文件或以编程方式提供这些值 |支持, 使用终端命令公开核心指标|
| RocketMQ | Java, C++, Go|	Pull model, 支持 TCP, JMS, OpenMessaging | 确保严格的消息排序，并可以优雅地扩展 |支持 |支持, 使用同步模式以避免消息丢失 | 支持 | 支持, 基于SQL92的属性过滤器表达式| 支持	|高性能和低延迟的文件存储 | 支持时间戳和偏移量两个标注 |不支持	 | 支持, Master-Slave 模型, 不需要其他工具包 | 支持 |开箱即用，用户只需要注意几个配置| 支持, 丰富的Web和终端命令来公开核心指标|



			
# 快速入门
这份快速入门指南是在本地计算机上启动RocketMQ消息系统来发送和接受消息的详细说明书。
# 先决条件
假定下列软件已经被安装：

1.64位操作系统，推荐使用Linux/Unix/Mac

2.64位JDK，版本1.8及以上

3.Maven3.2.x

4.Git
# 下载发行版本并构建
[点击这里](https://www.apache.org/dyn/closer.cgi?path=rocketmq/4.3.0/rocketmq-all-4.3.0-source-release.zip)下载4.3.0发行版的源代码。也可以从[这里](http://rocketmq.apache.org/release_notes/release-notes-4.3.0/)下载一个二进制发行版本。
现在执行下列命令来解压4.3.0发行版源代码，并构建二进制文件。

``` 
> unzip rocketmq-all-4.3.0-source-release.zip
> cd rocketmq-all-4.3.0/
> mvn -Prelease -all -DskipTests clean install -U
> cd distribution/target/apache-rocketmq
``` 
# 启动命名服务器

``` shell
> nohup sh bin/mqnamesrv &
> tail -f ~/logs/rocketmqlogs/namesrv.log
The Name Server boot success...
``` 



# 启动代理

``` 
> nohup sh bin/mqbroker-nlocalhost:9876 &
> tail -f ~/logs/rocketmqlogs/broker.log
The broker[%s,172.30.30.233:10911] boots uccess...
``` 
# 发送和接受消息
在发送/接受信息前，我们需要告诉客户端命名服务器的位置。RocketMQ提供了多种途径来实现它。简单起见，我们使用环境变量``` NAMESRV_ADDR``` ：

``` 
> export NAMESRV_ADDR=localhost:9876
> sh bin/tools.sh org.apache.rocketmq.example.quickstart.Producer
 SendResult [sendStatus=SEND_OK, msgId= ...
> sh bin/tools.sh org.apache.rocketmq.example.quickstart.Consumer
 ConsumeMessageThread_%d Receive New Messages: [MessageExt...
``` 
# 关闭服务器
``` bash
> sh bin/mqshutdown broker
The mqbroker(36695) is running...
Send shutdown request to mqbroker(36695) OK

> sh bin/mqshutdown namesrv
The mqnamesrv(36664) is running...
Send shutdown request to mqnamesrv(36664) OK
``` 






