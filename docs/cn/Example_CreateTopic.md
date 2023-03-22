# 创建主题

## 背景

RocketMQ 5.0 引入了 `TopicMessageType` 的概念，并且使用了现有的主题属性功能来实现它。

主题的创建是通过 `mqadmin` 工具来申明 `message.type` 属性。

## 使用案例

```shell
# default
sh ./mqadmin updateTopic -n <nameserver_address> -t <topic_name> -c DefaultCluster

# normal topic
sh ./mqadmin updateTopic -n <nameserver_address> -t <topic_name> -c DefaultCluster -a +message.type=NORMAL

# fifo topic
sh ./mqadmin updateTopic -n <nameserver_address> -t <topic_name> -c DefaultCluster -a +message.type=FIFO

# delay topic
sh ./mqadmin updateTopic -n <nameserver_address> -t <topic_name> -c DefaultCluster -a +message.type=DELAY

# transaction topic
sh ./mqadmin updateTopic -n <nameserver_address> -t <topic_name> -c DefaultCluster -a +message.type=TRANSACTION
```
