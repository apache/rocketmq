# Create Topic

## Background

The `TopicMessageType` concept is introduced in RocketMQ 5.0, using the existing topic attribute feature to implement it.

The topic is created by `mqadmin` tool declaring the `message.type` attribute.

## User Example

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
