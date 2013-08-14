### RocketMQ是什么？
RocketMQ是一款分布式、队列模型的消息中间件，具有以下特点：

* 能够保证严格的消息顺序
* 提供丰富的消息拉取模式
* 高效的订阅者水平扩展能力
* 实时的消息订阅机制
* 亿级消息堆积能力
* 极少的依赖，非常适合外部用户使用


----------

### 如何开始？
* [下载最新稳定版安装包](https://github.com/alibaba/RocketMQ/releases)
* Quick Start（[阿里内部用户](http://gitlab.alibaba-inc.com/middleware//rocketmq/wikis/RocketMQ_Notes) | [开源外部用户](https://github.com/alibaba/RocketMQ/wiki/Quick-Start)）
* 通过Wiki了解更多（[阿里内部用户](http://gitlab.alibaba-inc.com/middleware/rocketmq/wikis/home) | [开源外部用户](https://github.com/alibaba/RocketMQ/wiki)）

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

* [向我们提交建议、BUG、寻求技术帮助](https://github.com/alibaba/RocketMQ/issues/new)
* 欢迎参与RocketMQ项目，只需在Github上fork、pull request即可
* [到新浪微博交流RocketMQ（限开源外部用户）](http://q.weibo.com/1628465)

----------