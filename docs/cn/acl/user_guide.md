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
| topicPerms | topic=权限 | 各个Topic的权限，注意：该配置项后续会被弃用，topic权限配置请在resourcePerms中配置 |
| groupPerms | group=权限 | 各个ConsumerGroup的权限，注意：该配置项后续会被弃用，ConsumerGroup权限配置请在resourcePerms中配置 |
| resourcePerms |  | 各个Topic、ConsumerGroup的权限 |
| namespacePerms |  | 各个Namespace的权限 |

resourcePerms配置项含义：

| 字段 | 取值 | 含义 |
| --- | --- | --- |
| resource | 字符串 | topic或者consumerGroup的名称 |
| type | TOPIC;GROUP | 资源的类型，TOPIC表示当前权限设置的资源类型是Topic;GROUP表示当前权限设置的资源类型是ConsumerGroup; |
| namespace | 字符串 | 资源所属的Namespace |
| perm | DENY;PUB;SUB;PUB\|SUB | Topic或者ConsumerGroup的权限 |

namespacePerms配置项含义：

| 字段 | 取值 | 含义 |
| --- | --- | --- |
| namespace | 字符串 | Namespace名称 |
| topicPerm | DENY;PUB;SUB;PUB\|SUB | 默认的Topic权限，作用于当前Namespace |
| groupPerm | DENY;PUB;SUB;PUB\|SUB | 默认的ConsumerGroup权限，作用于当前Namespace |


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
（2）对于某个资源，如果有显性配置权限，则采用配置的权限；如果没有显性配置权限，判断该资源是否属于某个namespace，然后判断是否符合namespace中对资源的权限设置，如果该资源不属于任何namespace，则判断是否符合全局默认的topic权限或者group权限

## 5. 热加载修改后权限控制定义
RocketMQ的权限控制存储的默认实现是基于yml配置文件。用户可以动态修改权限控制定义的属性，而不需重新启动Broker服务节点。

## 6. 权限控制的使用限制
(1)如果ACL与高可用部署(Master/Slave架构)同时启用，那么需要在Broker Master节点的distribution/conf/plain_acl.yml配置文件中
设置全局白名单信息，即为将Slave节点的ip地址设置至Master节点plain_acl.yml配置文件的全局白名单中。

(2)如果ACL与高可用部署(多副本Dledger架构)同时启用，由于出现节点宕机时，Dledger Group组内会自动选主，那么就需要将Dledger Group组
内所有Broker节点的plain_acl.yml配置文件的白名单设置所有Broker节点的ip地址。

## 7. ACL mqadmin配置管理命令

### 7.1 更新ACL配置文件中“account”的属性值

该命令的示例如下：

sh mqadmin updateAclConfig -n 192.168.1.2:9876 -b 192.168.12.134:10911 -a RocketMQ -s 1234567809123 
-t topicA=DENY,topicD=SUB -g groupD=DENY,groupB=SUB

说明：为了兼容老版本在新版本中不会取消该命令，而是在用户执行完命令后反馈以下信息
```
updateAclConfig command will be deprecated. If you want to update secretKey whiteRemoteAddress 
admin defaultTopicPerm and defaultGroupPerm, updateAclAccount command is recommended. If you want to update resource perm,
updateAclResourcePerms command is recommended. If you want to update namespace perm, updateAclNamespacePerms command
is recommended.
```


| 参数 | 取值 | 含义 |
| --- | --- | --- |
| n | eg:192.168.1.2:9876 | namesrv地址(必填) |
| c | eg:DefaultCluster | 指定集群名称(与broker地址二选一) |
| b | eg:192.168.12.134:10911 | 指定broker地址(与集群名称二选一) |
| a | eg:RocketMQ | Access Key值(必填) |
| s | eg:1234567809123 | Secret Key值(可选) |
| m | eg:true | 是否管理员账户(可选) |
| w | eg:192.168.0.* | whiteRemoteAddress,用户IP白名单(可选) |
| i | eg:DENY;PUB;SUB;PUB\|SUB | defaultTopicPerm,默认Topic权限(可选) |
| u | eg:DENY;PUB;SUB;PUB\|SUB | defaultGroupPerm,默认ConsumerGroup权限(可选) |
| t | eg:topicA=DENY,topicD=SUB | topicPerms,各个Topic的权限(可选) |
| g | eg:groupD=DENY,groupB=SUB | groupPerms,各个ConsumerGroup的权限(可选) |

### 7.2 删除ACL配置文件里面的对应“account”
该命令的示例如下：

sh mqadmin deleteAccessConfig -n 192.168.1.2:9876 -c DefaultCluster -a RocketMQ

说明：如果指定的是集群名称，则会在集群中各个broker节点执行该命令；否则会在单个broker节点执行该命令。
其中，参数"a"为Access Key的值，用以标识唯一账户id，因此该命令的参数中指定账户id即可。

| 参数 | 取值 | 含义 |
| --- | --- | --- |
| n | eg:192.168.1.2:9876 | namesrv地址(必填) |
| c | eg:DefaultCluster | 指定集群名称(与broker地址二选一) |
| b | eg:192.168.12.134:10911 | 指定broker地址(与集群名称二选一) |
| a | eg:RocketMQ | Access Key的值(必填) |


### 7.3 更新ACL配置文件里面中的全局白名单
该命令的示例如下：

sh mqadmin updateGlobalWhiteAddr -n 192.168.1.2:9876 -b 192.168.12.134:10911 -g 10.10.154.1,10.10.154.2

说明：如果指定的是集群名称，则会在集群中各个broker节点执行该命令；否则会在单个broker节点执行该命令。
其中，参数"g"为全局IP白名的值，用以更新ACL配置文件中的“globalWhiteRemoteAddresses”字段的属性值。

| 参数 | 取值 | 含义 |
| --- | --- | --- |
| n | eg:192.168.1.2:9876 | namesrv地址(必填) |
| c | eg:DefaultCluster | 指定集群名称(与broker地址二选一) |
| b | eg:192.168.12.134:10911 | 指定broker地址(与集群名称二选一) |
| g | eg:10.10.154.1,10.10.154.2 | 全局IP白名单(必填) |

### 7.4 查询集群/Broker的ACL配置文件版本信息
该命令的示例如下：

sh mqadmin clusterAclConfigVersion -n 192.168.1.2:9876 -c DefaultCluster

说明：如果指定的是集群名称，则会在集群中各个broker节点执行该命令；否则会在单个broker节点执行该命令。

| 参数 | 取值 | 含义 |
| --- | --- | --- |
| n | eg:192.168.1.2:9876 | namesrv地址(必填) |
| c | eg:DefaultCluster | 指定集群名称(与broker地址二选一) |
| b | eg:192.168.12.134:10911 | 指定broker地址(与集群名称二选一) |

### 7.5 查询集群/Broker的ACL配置文件全部内容
该命令的示例如下：

sh mqadmin getAclConfig -n 192.168.1.2:9876 -c DefaultCluster

说明：如果指定的是集群名称，则会在集群中各个broker节点执行该命令；否则会在单个broker节点执行该命令。

| 参数 | 取值 | 含义 |
| --- | --- | --- |
| n | eg:192.168.1.2:9876 | namesrv地址(必填) |
| c | eg:DefaultCluster | 指定集群名称(与broker地址二选一) |
| b | eg:192.168.12.134:10911 | 指定broker地址(与集群名称二选一) |

### 7.6 更新AK的SK、用户IP白名单、管理员账户、默认Topic权限和默认consumerGroup权限
该命令的示例如下：

(1) 修改用户的SK

sh mqadmin updateAclAccount --namesrv 127.0.0.1:9876 --brokerAddr 127.0.0.1:10911
--accessKey RocketMQ --secretKey 1234567890

(2) 修改用户的admin属性

sh mqadmin updateAclAccount --namesrv 127.0.0.1:9876 --brokerAddr 127.0.0.1:10911
--accessKey RocketMQ --admin true

(4) 修改（全覆盖）用户IP白名单

sh mqadmin updateAclAccount --namesrv 127.0.0.1:9876 --brokerAddr 127.0.0.1:10911
--accessKey RocketMQ --whiteRemoteAddress 10.10.154.1

(5) 修改全局默认权限

sh mqadmin updateAclAccount --namesrv 127.0.0.1:9876 --brokerAddr 127.0.0.1:10911
--accessKey RocketMQ --defaultTopicPerm DENY

sh mqadmin updateAclAccount --namesrv 127.0.0.1:9876 --brokerAddr 127.0.0.1:10911
--accessKey RocketMQ --defaultGroupPerm DENY

注意：如果用户不存在则会创建该用户

| 参数 | 取值 | 含义 |
| --- | --- | --- |
| namesrv | eg:127.0.0.1:9876 | namesrv地址(必填) |
| clusterName | eg:DefaultCluster | 指定集群名称(与broker地址二选一) |
| brokerAddr | eg:192.168.12.134:10911 | 指定broker地址(与集群名称二选一) |
| accessKey | eg:RocketMQ | Access Key值(必填) |
| secretKey | eg:1234567809123 | Secret Key值(可选) |
| admin | eg:true | 是否管理员账户(可选) |
| whiteRemoteAddress | eg:192.168.0.* | whiteRemoteAddress,用户IP白名单(可选) |
| defaultTopicPerm | eg:DENY;PUB;SUB;PUB\|SUB | defaultTopicPerm,默认Topic权限(可选)，作用域是所有的namespace |
| defaultGroupPerm | eg:DENY;PUB;SUB;PUB\|SUB | defaultGroupPerm,默认ConsumerGroup权限(可选)，作用域是所有的namespace |

### 7.7 更新AK的resourcePerms
该命令的示例如下：

(1) 新增topicA的权限

sh mqadmin updateAclResourcePerms --namesrv 127.0.0.1:9876 
--brokerAddr 127.0.0.1:10911 --accessKey RocketMQ --secretKey 1234567890
--operation ADD --resource topicA --type TOPIC --namespace namespace1
--perm SUB

(2) 修改topicA的权限

sh mqadmin updateAclResourcePerms --namesrv 127.0.0.1:9876 
--brokerAddr 127.0.0.1:10911 --accessKey RocketMQ --secretKey 1234567890
--operation UPDATE --resource topicA --type TOPIC --namespace namespace1
--perm PUB|SUB

(3) 删除topicA的权限

sh mqadmin updateAclResourcePerms --namesrv 127.0.0.1:9876 
--brokerAddr 127.0.0.1:10911 --accessKey RocketMQ --secretKey 1234567890
--operation DELETE --resource topicA --type TOPIC --namespace namespace1

(4) 新增groupA的权限

sh mqadmin updateAclResourcePerms --namesrv 127.0.0.1:9876 
--brokerAddr 127.0.0.1:10911 --accessKey RocketMQ --secretKey 1234567890
--operation ADD --resource groupA --type GROUP --namespace namespace1
--perm PUB

(5) 修改groupA的权限

sh mqadmin updateAclResourcePerms --namesrv 127.0.0.1:9876 
--brokerAddr 127.0.0.1:10911 --accessKey RocketMQ --secretKey 1234567890
--operation UPDATE --resource groupA --type GROUP --namespace namespace1
--perm PUB|SUB

(6) 删除groupA的权限

sh mqadmin updateAclResourcePerms --namesrv 127.0.0.1:9876 
--brokerAddr 127.0.0.1:10911 --accessKey RocketMQ --secretKey 1234567890
--operation DELETE --resource groupA --type GROUP --namespace namespace1

| 参数 | 取值 | 含义 |
| --- | --- | --- |
| namesrv | eg:127.0.0.1:9876 | namesrv地址(必填) |
| clusterName | eg:DefaultCluster | 指定集群名称(与broker地址二选一) |
| brokerAddr | eg:192.168.12.134:10911 | 指定broker地址(与集群名称二选一) |
| accessKey | eg:RocketMQ | AccessKey值(必填) |
| secretKey | eg:1234567890 | SecretKey值(必填) |
| operation | eg:ADD;UPDATE;DELETE | 对resourcePerms的操作类型（必填） |
| resource | eg:topic1;group1 | 指定topic名称或者ConsumerGroup名称（必填） |
| type | eg:TOPIC;GROUP | 当操作对象是resource时，指定是topic还是ConsumerGroup（必填） |
| namespace | eg:namespace1 | 指定namespace名称（可选） |
| perm | eg:DENY;PUB;SUB;PUB\|SUB | resourcePerms中topic或者ConsumerGroup的权限（可选） |

### 7.8 更新AK的namespacePerms
该命令的示例如下：

(1) 新增namespace1的权限

sh mqadmin updateAclNamespacePerms --namesrv 127.0.0.1:9876 
--brokerAddr 127.0.0.1:10911 --accessKey RocketMQ --secretKey 1234567890
--operation ADD --namespace namespace1 --topicPerm SUB --groupPerm PUB

(2) 修改namespace1的权限
sh mqadmin updateAclNamespacePerms --namesrv 127.0.0.1:9876 
--brokerAddr 127.0.0.1:10911 --accessKey RocketMQ --secretKey 1234567890
--operation UPDATE --namespace namespace1 --topicPerm DENY 
--groupPerm PUB

(3) 删除namespace1的权限

sh mqadmin updateAclNamespacePerms --namesrv 127.0.0.1:9876 
--brokerAddr 127.0.0.1:10911 --accessKey RocketMQ --secretKey 1234567890
--operation DELETE --namespace namespace1

(4) 批量增加(新增namespace2和namespace3的权限，其topicPerm均为SUB，groupPerm均为PUB)

sh mqadmin updateAclNamespacePerms --namesrv 127.0.0.1:9876 
--brokerAddr 127.0.0.1:10911 --accessKey RocketMQ --secretKey 1234567890
--operation ADD --namespace namespace2，namespace3 --topicPerm SUB 
--groupPerm PUB

(5) 批量修改(将namespace2和namespace3的topicPerm和groupPerm修改为DENY)

sh mqadmin updateAclNamespacePerms --namesrv 127.0.0.1:9876 
--brokerAddr 127.0.0.1:10911 --accessKey RocketMQ --secretKey 1234567890
--operation UPDATE --namespace namespace2，namespace3 --topicPerm DENY 
--groupPerm DENY

(6) 批量删除

sh mqadmin updateAclNamespacePerms --namesrv 127.0.0.1:9876 
--brokerAddr 127.0.0.1:10911 --accessKey RocketMQ --secretKey 1234567890
--operation DELETE --namespace namespace2，namespace3

| 参数 | 取值 | 含义 |
| --- | --- | --- |
| namesrv | eg:127.0.0.1:9876 | namesrv地址(必填) |
| clusterName | eg:DefaultCluster | 指定集群名称(与broker地址二选一) |
| brokerAddr | eg:192.168.12.134:10911 | 指定broker地址(与集群名称二选一) |
| accessKey | eg:RocketMQ | AccessKey值(必填) |
| secretKey | eg:1234567890 | SecretKey值(必填) |
| operation | eg:ADD;UPDATE;DELETE | 对namespacePerms的操作类型（必填） |
| namespace | eg:namespace1 | 指定namespace名称（必填），如果是批量操作则使用逗号分割 |
| topicPerm | eg:DENY;PUB;SUB;PUB\|SUB | topicPerm，各个namespace默认的topic权限（可选） |
| groupPerm | eg:DENY;PUB;SUB;PUB\|SUB | groupPerm，各个namespace默认的ConsumerGroup权限（可选） |

### 7.9 根据accesskey获取其权限配置信息
该命令的使用示例如下：

(1) 获取RocketMQ的权限配置

sh mqadmin getAccesskeyConfig --namesrv 127.0.0.1:9876 
--brokerAddr 127.0.0.1:10911 --accessKey RocketMQ

| 参数 | 取值 | 含义 |
| --- | --- | --- |
| namesrv | eg:127.0.0.1:9876 | namesrv地址(必填) |
| clusterName | eg:DefaultCluster | 指定集群名称(与broker地址二选一) |
| brokerAddr | eg:192.168.12.134:10911 | 指定broker地址(与集群名称二选一) |
| accessKey | eg:RocketMQ | Access Key值(必填) |


**特别注意**开启Acl鉴权认证后导致Master/Slave和Dledger模式下Broker同步数据异常的问题，
在社区[4.5.1]版本中已经修复，具体的PR链接为：#1149。
