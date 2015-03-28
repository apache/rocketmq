### 操作系统调优
在生产环境部署Broker前，必须要执行os.sh，对操作系统进行调优

**P.S: os.sh只能执行一次，需要sudo root权限**

### 启动broker
* Unix平台

	`nohup sh mqbroker &`

* Windows平台（仅支持64位）

	`mqbroker.exe`

### 关闭broker
	sh mqshutdown broker

### 启动Name Server
* Unix平台

	`nohup sh mqnamesrv &`

* Windows平台（仅支持64位）

	`mqnamesrv.exe`

### 关闭Name Server
	sh mqshutdown namesrv

### 更新或创建Topic
	sh mqadmin updateTopic -b 127.0.0.1:10911 -t TopicA

### 更新或创建订阅组
	sh mqadmin updateSubGroup -b 127.0.0.1:10911 -g SubGroupA