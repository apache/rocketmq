﻿Apache RocketMQ开发者指南
--------

##### 这个开发者指南旨在帮助您快速了解并使用 Apache RocketMQ

### 1. 概念和特性

- [概念(Concept)](concept.md)：介绍RocketMQ的基本概念模型。

- [特性(Features)](features.md)：介绍RocketMQ实现的功能特性。 


### 2. 架构设计

- [架构(Architecture)](architecture.md)：介绍RocketMQ部署架构和技术架构。

- [设计(Design)](design.md)：介绍RocketMQ关键机制的设计原理，主要包括消息存储、通信机制、消息过滤、负载均衡、事务消息等。


### 3. 样例

- [样例(Example)](RocketMQ_Example.md) ：介绍RocketMQ的常见用法，包括基本样例、顺序消息样例、延时消息样例、批量消息样例、过滤消息样例、事务消息样例等。

### 4. 最佳实践
- [最佳实践（Best Practice）](best_practice.md)：介绍RocketMQ的最佳实践，包括生产者、消费者、Broker以及NameServer的最佳实践，客户端的配置方式以及JVM和linux的最佳参数配置。
- [消息轨迹指南(Message Trace)](msg_trace/user_guide.md)：介绍RocketMQ消息轨迹的使用方法。
- [权限管理(Auth Management)](acl/user_guide.md)：介绍如何快速部署和使用支持权限控制特性的RocketMQ集群。
- [自动主从切换快速开始](controller/quick_start.md)：RocketMQ 5.0 自动主从切换快速开始。
- [自动主从切换部署升级指南](controller/deploy.md)：RocketMQ 5.0 自动主从切换部署升级指南。
- [Proxy 部署指南](proxy/deploy_guide.md)：介绍如何部署Proxy (包括 `Local` 模式和 `Cluster` 模式).

### 5. 运维管理
- [集群部署(Operation)](operation.md)：介绍单Master模式、多Master模式、多Master多slave模式等RocketMQ集群各种形式的部署方法以及运维工具mqadmin的使用方式。

### 6. RocketMQ 5.0 新特性

- [POP消费](https://github.com/apache/rocketmq/wiki/%5BRIP-19%5D-Server-side-rebalance,--lightweight-consumer-client-support)
- [StaticTopic](statictopic/RocketMQ_Static_Topic_Logic_Queue_设计.md)
- [BatchConsumeQueue](https://github.com/apache/rocketmq/wiki/RIP-26-Improve-Batch-Message-Processing-Throughput)
- [自动主从切换](controller/design.md)
- [BrokerContainer](BrokerContainer.md)
- [SlaveActingMaster模式](SlaveActingMasterMode.md)
- [Grpc Proxy](../../proxy/README.md)

### 7. API Reference（待补充）

- [DefaultMQProducer API Reference](client/java/API_Reference_DefaultMQProducer.md)







