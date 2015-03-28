### RocketMQ是什么？[![Build Status](https://travis-ci.org/alibaba/RocketMQ.svg?branch=develop)](https://travis-ci.org/alibaba/RocketMQ)
RocketMQ是一款分布式、队列模型的消息中间件，具有以下特点：

* 支持严格的消息顺序
* 支持Topic与Queue两种模式
* 亿级消息堆积能力
* 比较友好的分布式特性
* 同时支持Push与Pull方式消费消息
* 历经多次天猫双十一海量消息考验

----------

### 如何开始？`必读`
* [下载最新版安装包](https://github.com/alibaba/RocketMQ/releases)
* [向开发者索要最新文档](https://github.com/alibaba/RocketMQ/issues/1)
* [`在阿里云上使用RocketMQ`](http://www.aliyun.com/product/ons)，可以免去繁琐的部署运维、成本。
* [与阿里巴巴的流计算框架JStorm配合使用](https://github.com/alibaba/jstorm)
* [阿里巴巴mysql数据库binlog的增量订阅&消费组件canal配合使用](https://github.com/alibaba/canal)


----------

### 开源协议
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) 2010-2013 Alibaba Group Holding Limited

----------

### 开发规范
* 代码使用Eclipse代码样式格式化，提交代码前须格式化[rocketmq.java.code.style.xml](https://github.com/alibaba/RocketMQ/blob/master/docs/rocketmq.java.code.style.xml)
* Java源文件使用Unix换行、UTF-8文件编码
* 请在git clone命令之前执行`git config --global core.autocrlf false`，确保本地代码使用Unix换行格式
* 请在develop分支上开发
* 每次提交须有Issue关联，可在git提交注释中增加`#issue号码`进行关联

----------

### 联系我们
* [交流&建议](https://groups.google.com/forum/?hl=en#!forum/rocketmq)
* [Issues&Bugs](https://github.com/alibaba/RocketMQ/issues/new)
* [到新浪微博交流RocketMQ](http://q.weibo.com/1628465)
* 加入QQ群交流，[5776652](http://url.cn/Knxm0o)

----------

### 阿里中间件团队招兵买马（加入我们）

阿里中间件团队诚邀IT精英加盟，2014年天猫双十一创下了571亿的订单交易总额，对底层的中间件系统提出了越来越高的挑战，同时中间件产品开始发力云计算，以PAAS服务形式提供给广大电商商家ISV，互联网创业，移动互联网创业，以及国内大型企业意欲转向互联网架构的用户。阿里中间件团队拥有完整的中间件产品线，包括消息中间件，分布式服务调用，分布式事务中间件，分布式数据库，流计算，分布式调用跟踪，负载均衡等产品。我们需要您能有一定的C/C++，Java经验，并能了解存储系统，网络编程，对互联网底层架构常见容错方式能有了解，同时希望您能具有独立构建一个中间件产品的能力。

工作地点：`杭州/北京`

如果您对互联网技术架构感兴趣，请不要犹豫，发简历给我 `shijia.wxr@taobao.com`


