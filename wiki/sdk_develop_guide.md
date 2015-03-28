### 开发一个RocketMQ客户端（例如C++客户端）需要做哪些工作
* 需要开发一个长连接的通信层，用来与Broker、Name Server通信，并负责编码解码。
* 需要编写Producer、Consumer的负载均衡

### 通信协议介绍
	协议格式 <length> <header length> <header data> <body data>
	           1        2               3             4
	协议分4部分，含义分别如下
	    1、大端4个字节整数，等于2、3、4长度总和
	    2、大端4个字节整数，等于3的长度
	    3、使用json序列化数据
	    4、应用自定义二进制序列化数据

> 第三部分，Header格式

	{
	  "code": 0,
	  "language": "JAVA",
	  "version": 0,
	  "opaque": 0,
	  "flag": 1,
	  "remark": "hello, I am respponse /127.0.0.1:27603",
	  "extFields": {
	    "count": "0",
	    "messageTitle": "HelloMessageTitle"
	  }
	}



### 从Name Server获取Broker列表，【请求部分】


### 从Name Server获取Broker列表，【应答部分】



