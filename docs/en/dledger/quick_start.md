# Dledger Quick Deployment
---
### preface
This document is mainly introduced for how to build and deploy auto failover RocketMQ cluster based on DLedger.

For detailed new cluster deployment and old cluster upgrade document, please refer to [Deployment Guide](deploy_guide.md).

### 1. Build from source code
Build phase contains two parts, first, build DLedger, then build RocketMQ.

#### 1.1 Build DLedger

```shell
$ git clone https://github.com/openmessaging/dledger.git
$ cd dledger
$ mvn clean install -DskipTests
```

#### 1.2 Build RocketMQ

```shell
$ git clone https://github.com/apache/rocketmq.git
$ cd rocketmq
$ git checkout -b store_with_dledger origin/store_with_dledger
$ mvn -Prelease-all -DskipTests clean install -U
```

### 2. Quick Deployment

after build successful

```shell
#{rocketmq-version} replace with rocketmq actual version. example: 5.0.0-SNAPSHOT
$ cd distribution/target/rocketmq-{rocketmq-version}/rocketmq-{rocketmq-version}
$ sh bin/dledger/fast-try.sh start
```

if the above commands executed successfully, then check cluster status by using mqadmin operation commands.

```shell
$ sh bin/mqadmin clusterList -n 127.0.0.1:9876
```

If everything goes well, the following content will appear:

![ClusterList](https://img.alicdn.com/5476e8b07b923/TB11Z.ZyCzqK1RjSZFLXXcn2XXa)

（BID is 0 indicate Master, the others are Follower）

After startup successful, producer can produce message, and then test failover scenario.

Stop cluster fastly, execute the following command:

```shell
$ sh bin/dledger/fast-try.sh stop
```

Quick deployment, default configuration is in directory conf/dledger, default storage path is /tmp/rmqstore.


### 3. Failover

After successful deployment, kill Leader process(as the above example, kill process that binds port 30931), about 10 seconds elapses, use clusterList command check cluster's status, Leader switch to another node.





