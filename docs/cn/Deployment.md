# 部署架构和设置步骤

## 集群的设置

### 1 单master模式

这是最简单但也是最危险的模式，一旦broker服务器重启或宕机，整个服务将不可用。 建议在生产环境中不要使用这种部署方式，在本地测试和开发可以选择这种模式。 以下是构建的步骤。

**1）启动NameServer**

```shell
### 第一步启动namesrv
$ nohup sh mqnamesrv &
 
### 验证namesrv是否启动成功
$ tail -f ~/logs/rocketmqlogs/namesrv.log
The Name Server boot success...
```

我们可以在namesrv.log 中看到'The Name Server boot success..'，表示NameServer 已成功启动。

**2）启动Broker**

```shell
### 第一步先启动broker
$ nohup sh bin/mqbroker -n localhost:9876 &

### 验证broker是否启动成功, 比如, broker的ip是192.168.1.2 然后名字是broker-a
$ tail -f ~/logs/rocketmqlogs/Broker.log 
The broker[broker-a,192.169.1.2:10911] boot success...
```

我们可以在 Broker.log 中看到“The broker[brokerName,ip:port] boot success..”，这表明 broker 已成功启动。

### 2 多Master模式

该模式是指所有节点都是master主节点（比如2个或3个主节点），没有slave从节点的模式。 这种模式的优缺点如下：

- 优点: 
  1. 配置简单。
  2. 一个master节点的宕机或者重启（维护）对应用程序没有影响。
  3. 当磁盘配置为RAID10时，消息不会丢失，因为RAID10磁盘非常可靠，即使机器不可恢复（消息异步刷盘模式的情况下，会丢失少量消息；如果消息是同步刷盘模式，不会丢失任何消息）。
  4. 在这种模式下，性能是最高的。
- 缺点:
  1. 单台机器宕机时，本机未消费的消息，直到机器恢复后才会订阅，影响消息实时性。

多Master模式的启动步骤如下：

**1）启动 NameServer**

```shell
### 第一步先启动namesrv
$ nohup sh mqnamesrv &
 
### 验证namesrv是否启动成功
$ tail -f ~/logs/rocketmqlogs/namesrv.log
The Name Server boot success...
```

**2）启动 Broker 集群**

```shell
### 比如在A机器上启动第一个Master，假设配置的NameServer IP为：192.168.1.1
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-noslave/broker-a.properties &
 
### 然后在机器B上启动第二个Master，假设配置的NameServer IP是：192.168.1.1
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-noslave/broker-b.properties &

...
```

上面显示的启动命令用于单个NameServer的情况。对于多个NameServer的集群，broker 启动命令中-n参数后面的地址列表用分号隔开，例如 192.168.1.1:9876;192.161.2:9876

### 3 多Master多Slave模式-异步复制

每个主节点配置多个从节点，多对主从。HA采用异步复制，主节点和从节点之间有短消息延迟（毫秒）。这种模式的优缺点如下：

- 优点:
  1. 即使磁盘损坏，也只会丢失极少的消息，不影响消息的实时性能。
  2. 同时，当主节点宕机时，消费者仍然可以消费从节点的消息，这个过程对应用本身是透明的，不需要人为干预。
  3. 性能几乎与多Master模式一样高。
- 缺点:
  1. 主节点宕机、磁盘损坏时，会丢失少量消息。

多主多从模式的启动步骤如下：

**1）启动 NameServer**

```shell
### 第一步先启动namesrv
$ nohup sh mqnamesrv &
 
### 验证namesrv是否启动成功
$ tail -f ~/logs/rocketmqlogs/namesrv.log
The Name Server boot success...
```

**2）启动 Broker 集群**

```shell
### 例如在A机器上启动第一个Master，假设配置的NameServer IP为：192.168.1.1，端口为9876。
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-async/broker-a.properties &
 
### 然后在机器B上启动第二个Master，假设配置的NameServer IP为：192.168.1.1，端口为9876。
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-async/broker-b.properties &
 
### 然后在C机器上启动第一个Slave，假设配置的NameServer IP为：192.168.1.1，端口为9876。
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-async/broker-a-s.properties &
 
### 最后在D机启动第二个Slave，假设配置的NameServer IP为：192.168.1.1，端口为9876。
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-async/broker-b-s.properties &
```

上图显示了 2M-2S-Async 模式的启动命令，类似于其他 nM-nS-Async 模式。

### 4 多Master多Slave模式-同步双写

这种模式下，每个master节点配置多个slave节点，有多对Master-Slave。HA采用同步双写，即只有消息成功写入到主节点并复制到多个从节点，才会返回成功响应给应用程序。

这种模式的优缺点如下：

- 优点: 
  1. 数据和服务都没有单点故障。
  2. 在master节点关闭的情况下，消息也没有延迟。
  3. 服务可用性和数据可用性非常高；
- 缺点:
  1. 这种模式下的性能略低于异步复制模式（大约低 10%）。
  2. 发送单条消息的RT略高，目前版本，master节点宕机后，slave节点无法自动切换到master。

启动步骤如下：

**1）启动NameServer**

```shell
### 第一步启动namesrv
$ nohup sh mqnamesrv &
 
### 验证namesrv是否启动成功
$ tail -f ~/logs/rocketmqlogs/namesrv.log
The Name Server boot success...
```

**2）启动 Broker 集群**

```shell
### 例如在A机器上启动第一个Master，假设配置的NameServer IP为：192.168.1.1，端口为9876。
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-sync/broker-a.properties &
 
### 然后在B机器上启动第二个Master，假设配置的NameServer IP为：192.168.1.1，端口为9876。
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-sync/broker-b.properties &
 
### 然后在C机器上启动第一个Slave，假设配置的NameServer IP为：192.168.1.1，端口为9876。
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-sync/broker-a-s.properties &
 
### 最后在D机启动第二个Slave，假设配置的NameServer IP为：192.168.1.1，端口为9876。
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-sync/broker-b-s.properties &
```

上述Master和Slave是通过指定相同的config命名为“brokerName”来配对的，master节点的brokerId必须为0，slave节点的brokerId必须大于0。