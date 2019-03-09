# 消息查询（Message Query）

RocketMQ支持两个维度的消息查询，分别是“按消息ID查询”和“按消息键值查询”。

##1.按消息ID查询（Query Message By MessageID）

RocketMQ中的消息ID总长度为16个字节，包括代理地址（IP地址和端口）和CommitLog 偏移。在RocketMQ中，具体的方法是客户机通过消息ID解析代理的地址（IP地址和端口）和CommitLog相对消息ID的偏移地址。然后将这两个请求封装到一个RPC请求中，最后通过通信层发送（业务请求代码：VIEW_message_by_id）。代理通过使用CommitLog偏移量和大小来读取消息，从而在CommitLog中找到真正的消息，然后返回，这就是QueryMessageProcessor的工作方式。

##2.按消息键值查询（Query Message By Message Key）

“按消息键值查询”主要基于RocketMQ的索引文件。索引文件的逻辑结构类似于JDK中HashMap的实现。 索引文件的具体结构如下：

        rocketmq/docs/en/images/rocketmq_design_message_query.png
索引文件通过“按消息键值查询”为用户提供查询服务。索引文件存储在$HOME\store\index${fileName}中，文件名以创建时的时间戳命名。文件大小是固定的，即420,000,040字节（40 + 5000，000*4 + 20，000，000*20）。如果在消息的属性中设置了UNIQ_KEY，那么“topic+'＃'+ UNIQ_KEY”将用作索引。同样，如果在消息的属性中设置了KEYS（多个KEY应该用空格分隔），那么“topic +'＃'+ KEY”将用作索引。

索引数据包含四个字段：Key Hash，CommitLog offset，Timestamp和NextIndex offset，共20个字节。如果索引数据的Key Hash值与先前索引数据的Key Hash值相同，则索引数据的NextIndex偏移将指向先前的索引数据。如果发生哈希冲突，则NextIndex偏移量字段可用链表链接所有冲突的索引。时间戳记录的是两个storeTimestamp之间的时间差，而不是特定时间。整个索引文件的结构如图所示。 Header用于存储一些常规统计信息，需要40个字节。 4×5000，000字节的时隙表不保存真正的索引数据，而是保存与每个时隙对应的单链表的头部。 20 * 20，000，000的索引链表是真正的索引数据，即索引文件可以容纳20，000，000个索引。

“按消息键值查询”的具体方法是使用主题和消息密钥在IndexFile中查找记录，然后根据该记录中的CommitLog偏移量从CommitLog文件中读取消息。