# Version记录
| 时间 | 主要内容 | 作者 |
| --- | --- | --- |
| 2022-01-27 | 初版，包括需求背景、兼容性影响、重要业务逻辑和后续扩展性考虑 | sunxi92 |

中文文档在描述特定专业术语时，仍然使用英文。
# 需求背景
RocketMQ ACL特性目前只支持单个ACL配置文件，当存在很多用户时该配置文件会非常大，因此提出支持多ACL配置文件的想法。
如果支持该特性那么也方便对RocketMQ用户进行分类。

# 兼容性影响
当前在支持多ACL配置文件特性的设计上是向前兼容的。

# 重要业务逻辑
## 1. ACL配置文件存储路径
ACL配置文件夹是在RocketMQ安装目录下的conf/acl目录中，也可以在该路径新建子目录并在子目录中新建ACL配置文件，同时也保留了之前默认的配置文件conf/plain_acl.yml。
注意：目前用户还不能自定义配置文件目录。
## 2. ACL配置文件更新
热感知：当检测到ACL配置文件改动会自动刷新数据，判断ACL配置文件是否发生变化的依据是文件的修改时间是否发生变化
## 3. RocketMQ Broker缓存ACL配置信息数据结构设计
- aclPlainAccessResourceMap

aclPlainAccessResourceMap是个Map类型，用来缓存所有ACL配置文件的权限数据，其中key表示ACL配置文件的绝对路径，
value表示相应配置文件中的权限数据，需要注意的是value也是一个Map类型，其中key是String类型表示AccessKey，value是PlainAccessResource类型。
- accessKeyTable

accessKeyTable是个Map类型，用来缓存AccessKey和ACL配置文件的映射关系，其中key表示AccessKey，value表示ACL配置文件的绝对路径。
- globalWhiteRemoteAddressStrategy

globalWhiteRemoteAddressStrategy用来缓存所有ACL配置文件的全局白名单。
- globalWhiteRemoteAddressStrategyMap

globalWhiteRemoteAddressStrategyMap是个Map类型，用来缓存ACL配置文件和全局白名单的映射关系
- dataVersionMap

dataVersionMap是个Map类型，用来缓存所有ACL配置文件的DataVersion，其中key表示ACL配置文件的绝对路径，value表示该配置文件对应的DataVersion。
## 4.加载和监控ACL配置文件
### 4.1 加载ACL配置文件
- load()

load()方法会获取"RocketMQ安装目录/conf"目录（包括该目录的子目录）和"rocketmq.acl.plain.file"下所有ACL配置文件，然后遍历这些文件读取权限数据和全局白名单。
- load(String aclFilePath)

load(String aclFilePath)方法完成加载指定ACL配置文件内容的功能，将配置文件中的全局白名单globalWhiteRemoteAddresses和用户权限accounts加载到缓存中，
这里需要注意以下几点：

（1）判断缓存中该配置文件的全局白名单globalWhiteRemoteAddresses和用户权限accounts数据是否为空，如果不为空则需要注意删除文件原有数据

（2）相同的accessKey只允许存在在一个ACL配置文件中
### 4.2 监控ACL配置文件
watch()方法用来监控"RocketMQ安装目录/conf"目录下所有ACL配置文件和"rocketmq.acl.plain.file"是否发生变化，变化考虑两种情况：一种是ACL配置文件的数量发生变化，
此时会调用load()方法重新加载所有配置文件的数据；一种是配置文件的内容发生变化；具体完成监控ACL配置文件变化的是AclFileWatchService服务，
该服务是一个线程，当启动该服务后它会以WATCH_INTERVAL（该参数目前设置为5秒，目前还不能在Broker配置文件中设置）的时间间隔来执行其核心逻辑。在该服务中会记录其监控的ACL配置文件目录aclPath、
ACL配置文件的数量aclFilesNum、所有ACL配置文件绝对路径fileList以及每个ACL配置文件最近一次修改的时间fileLastModifiedTime
（Map类型，key为ACL配置文件的绝对路径，value为其最近一次修改时间）。
该服务的核心逻辑如下：
获取ACL配置文件数量并和aclFilesNum进行比较是否相等，如果不相等则更新aclFilesNum和fileList并调用load()方法重新加载所有配置文件；
如果相等则遍历每个ACL配置文件，获取其最近一次修改的时间，并将该时间与fileLastModifiedTime中记录的时间进行比较，如果不相等则表示该文件发生过修改，
此时调用load(String aclFilePath)方法重新加载该配置文件。

## 5. 权限数据相关操作修改
（1） updateAclConfigFileVersion(Map<String, Object> updateAclConfigMap)

添加对缓存dataVersionMap的修改

（2）updateAccessConfig(PlainAccessConfig plainAccessConfig)

将该方法原有的逻辑修改为：首先判断accessKeyTable中是否包含待修改的accessKey，如果包含则根据accessKey来获取其对应的ACL配置文件绝对路径，
再根据该路径更新aclPlainAccessResourceMap中缓存的数据，最后将该ACL配置文件中的数据写回原文件；如果不包含则会将数据写到"rocketmq.acl.plain.file"配置文件中，
然后更新accessKeyTable和aclPlainAccessResourceMap，最后最后将该ACL配置文件中的数据写回原文件。

（3）deleteAccessConfig(String accesskey)

将该方法原有的逻辑修改为：判断accessKeyTable中是否存在accesskey，如果不存在则返回false，否则将其删除并将修改后的数据写回原文件。

（4）getAllAclConfig()

fileList中存储了所有ACL配置文件的绝对路径，遍历fileList分别从各ACL配置文件中读取数据并组装返回

（5）updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList, String fileName)

该方法是新增的，完成功能是修改指定ACL配置文件的全局白名单，为后续添加相关运维命令做准备
## 6. ACL相关运维命令修改
（1）ClusterAclConfigVersionListSubCommand

将printClusterBaseInfo(final DefaultMQAdminExt defaultMQAdminExt, final String addr)方法原有的逻辑修改为：
获取全部的ACL配置文件的DataVersion并输出。注意：获取的全部ACL配置文件的DataVersion集合可能为空，这里需要添加判断

（2）GetBrokerAclConfigResponseHeader

在GetBrokerAclConfigResponseHeader中新增allAclFileVersion字段，它是个Map类型，其key表示ACL配置文件的绝对路径，value表示对应ACL配置文件的DataVersion

（3）ClusterAclVersionInfo

在ClusterAclVersionInfo中废弃了aclConfigDataVersion属性，增加了allAclConfigDataVersion属性，该属性是个Map类型，用来存储所有ACL配置文件的版本数据，
其中key表示ACL配置文件的绝对路径，value表示对应ACL配置文件的DataVersion

## 7. 关于ACL配置文件DataVersion存储修改

在原来版本中ACL权限数据存储在一个配置文件中，所以只记录了该配置文件的DataVersion，而现在需要支持多个配置文件特性，每个配置文件都有自己的DataVersion，
为了能够准确记录所有配置文件的DataVersion，需要调整相关类型的属性、接口及方法。

（1）PlainPermissionManager

对PlainPermissionManager属性的修改具体如下：

- 废弃dataVersion属性，该属性在历史版本中是用来存来存储默认ACL配置文件的DataVersion

- 新增dataVersionMap属性用来缓存所有ACL配置文件的DataVersion，它是一个Map类型，其key表示ACL配置文件的绝对路径，value表示对应配置文件的DataVersion

（2）AccessValidator

对AccessValidator的修改如下：

- 废弃String getAclConfigVersion();，该接口原来是获取ACL配置文件文件的版本数据

- 新增Map<String, DataVersion> getAllAclConfigVersion();该接口是用来获取所有ACL配置文件的版本数据，接口会返回一个Map类型数据，
key表示各ACL配置文件的绝对路径，value表示对应配置文件的版本数据

（3）PlainAccessValidator

由于PlainAccessValidator实现了AccessValidator接口，所以相应地增加了getAllAclConfigVersion()方法

# 后续扩展性考虑
1.目前的修改只支持ACL配置文件存储在"RocketMQ安装目录/conf"目录下，后续可以考虑支持多目录；

2.目前ACL配置文件路径是不支持让用户指定，后续可以考虑让用户指定指定ACL配置文件的存储路径

3.当前updateGlobalWhiteAddrsConfig命令只支持修改"rocketmq.acl.plain.file"文件中全局白名单，
后续可以扩展为修改指定ACL配置文件的全局白名单（如果参数中没有传ACL配置文件则会修改"rocketmq.acl.plain.file"文件）

4.目前ACL数据中的secretKey是以明文形式存储在文件中，在一些对此类信息敏感的行业是不允许以明文落地，后续可以考虑安全性问题

5.目前ACL数据存储只支持文件形式存储，后续可以考虑增加数据库存储



