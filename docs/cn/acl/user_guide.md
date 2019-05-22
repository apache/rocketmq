# 权限控制
----


## 1.权限控制特性介绍
权限控制（ACL）主要为RocketMQ提供Topic资源级别的用户访问控制。用户在使用RocketMQ权限控制时，可以在Client客户端通过 RPCHook注入AccessKey和SecretKey签名；同时，将对应的权限控制属性（包括Topic访问权限、IP白名单和AccessKey和SecretKey签名等）设置在distribution/conf/plain_acl.yml的配置文件中。Broker端对AccessKey所拥有的权限进行校验，校验不过，抛出异常；
ACL客户端可以参考：**org.apache.rocketmq.example.simple**包下面的**AclClient**代码。

## 2. 权限控制的定义与属性值
### 2.1权限定义
对RocketMQ的Topic资源访问权限控制定义主要如下表所示，分为以下四种


| 权限 | 含义 |
| --- | --- |
| DENY | 拒绝 |
| ANY | PUB 或者 SUB 权限 |
| PUB | 发送权限 |
| SUB | 订阅权限 |

### 2.2 权限定义的关键属性
| 字段 | 取值 | 含义 |
| --- | --- | --- |
| globalWhiteRemoteAddresses | \*;192.168.\*.\*;192.168.0.1 | 全局IP白名单 |
| accessKey | 字符串 | Access Key |
| secretKey | 字符串 | Secret Key |
| whiteRemoteAddress | \*;192.168.\*.\*;192.168.0.1 | 用户IP白名单 |
| admin | true;false | 是否管理员账户 |
| defaultTopicPerm | DENY;PUB;SUB;PUB\|SUB | 默认的Topic权限 |
| defaultGroupPerm | DENY;PUB;SUB;PUB\|SUB | 默认的ConsumerGroup权限 |
| topicPerms | topic=权限 | 各个Topic的权限 |
| groupPerms | group=权限 | 各个ConsumerGroup的权限 |

具体可以参考**distribution/conf/plain_acl.yml**配置文件

## 3. 支持权限控制的集群部署
在**distribution/conf/plain_acl.yml**配置文件中按照上述说明定义好权限属性后，打开**aclEnable**开关变量即可开启RocketMQ集群的ACL特性。这里贴出Broker端开启ACL特性的properties配置文件内容：
```
brokerClusterName=DefaultCluster
brokerName=broker-a
brokerId=0
deleteWhen=04
fileReservedTime=48
brokerRole=ASYNC_MASTER
flushDiskType=ASYNC_FLUSH
storePathRootDir=/data/rocketmq/rootdir-a-m
storePathCommitLog=/data/rocketmq/commitlog-a-m
autoCreateSubscriptionGroup=true
## if acl is open,the flag will be true
aclEnable=true
listenPort=10911
brokerIP1=XX.XX.XX.XX1
namesrvAddr=XX.XX.XX.XX:9876
```

## 4. 权限控制主要流程
ACL主要流程分为两部分，主要包括权限解析和权限校验。

### 4.1 权限解析
Broker端对客户端的RequestCommand请求进行解析，拿到需要鉴权的属性字段。
主要包括：
（1）AccessKey：类似于用户名，代指用户主体，权限数据与之对应；
（2）Signature：客户根据 SecretKey 签名得到的串，服务端再用SecretKey进行签名验证；

### 4.2 权限校验
Broker端对权限的校验逻辑主要分为以下几步：
（1）检查是否命中全局 IP 白名单；如果是，则认为校验通过；否则走 2；
（2）检查是否命中用户 IP 白名单；如果是，则认为校验通过；否则走 3；
（3）校验签名，校验不通过，抛出异常；校验通过，则走 4；
（4）对用户请求所需的权限 和 用户所拥有的权限进行校验；不通过，抛出异常； 
用户所需权限的校验需要注意已下内容：
（1）特殊的请求例如 UPDATE_AND_CREATE_TOPIC 等，只能由 admin 账户进行操作；
（2）对于某个资源，如果有显性配置权限，则采用配置的权限；如果没有显性配置权限，则采用默认的权限；

## 5. 热加载修改后权限控制定义
RocketMQ的权限控制存储的默认实现是基于yml配置文件。用户可以动态修改权限控制定义的属性，而不需重新启动Broker服务节点。

## 6. 权限控制的使用限制
(1)如果ACL与高可用部署(Master/Slave架构)同时启用，那么需要在Broker Master节点的distribution/conf/plain_acl.yml配置文件中
设置全局白名单信息，即为将Slave节点的ip地址设置至Master节点plain_acl.yml配置文件的全局白名单中。

(2)如果ACL与高可用部署(多副本Dledger架构)同时启用，由于出现节点宕机时，Dledger Group组内会自动选主，那么就需要将Dledger Group组
内所有Broker节点的plain_acl.yml配置文件的白名单设置所有Broker节点的ip地址。

**特别注意**在[4.5.0]版本中即使使用上面所述的白名单也无法解决开启ACL的问题，解决该问题的[PR链接](https://github.com/apache/rocketmq/pull/1149)

