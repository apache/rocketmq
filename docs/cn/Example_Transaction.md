# 事务消息示例

## 1 事务消息状态
 事务消息有三个状态:
- TransactionStatus.CommitTransaction: 提交事务，这意味着允许消费者消费此消息。
- TransactionStatus.RollbackTransaction: 回滚事务，这意味着消息将被删除，不允许使用。  
- TransactionStatus.Unknown: 中间状态，这意味着MQ需要检查，以确定状态。

## 2 发送事务性消息示例

### 2.1 创建事务性生产者
使用```TransactionMQProducer```类 创建生产者客户端，指定唯一的 ```ProducerGroup```，并可以设置自定义线程池来处理检查请求。执行本地事务后，需要根据执行结果回复 MQ，回复状态如上段所述。
```java
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import java.util.List;
public class TransactionProducer {
   public static void main(String[] args) throws MQClientException, InterruptedException {
       TransactionListener transactionListener = new TransactionListenerImpl();
       TransactionMQProducer producer = new TransactionMQProducer("please_rename_unique_group_name");
       ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
           @Override
           public Thread newThread(Runnable r) {
               Thread thread = new Thread(r);
               thread.setName("client-transaction-msg-check-thread");
               return thread;
           }
       });
       producer.setExecutorService(executorService);
       producer.setTransactionListener(transactionListener);
       producer.start();
       String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
       for (int i = 0; i < 10; i++) {
           try {
               Message msg =
                   new Message("TopicTest1234", tags[i % tags.length], "KEY" + i,
                       ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
               SendResult sendResult = producer.sendMessageInTransaction(msg, null);
               System.out.printf("%s%n", sendResult);
               Thread.sleep(10);
           } catch (MQClientException | UnsupportedEncodingException e) {
               e.printStackTrace();
           }
       }
       for (int i = 0; i < 100000; i++) {
           Thread.sleep(1000);
       }
       producer.shutdown();
   }
}
```

### 2.2 实现TransactionListener 接口
```executeLocalTransaction```方法用于在发送half消息成功时执行本地事务。它返回上一节中提到的三个事务状态之一。

```checkLocalTransaction``` 方法用于检查本地事务状态并响应 MQ 检查请求。它同样返回上一节中提到的三个事务状态之一。

```java
public class TransactionListenerImpl implements TransactionListener {
  private AtomicInteger transactionIndex = new AtomicInteger(0);
  private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();
  @Override
  public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
      int value = transactionIndex.getAndIncrement();
      int status = value % 3;
      localTrans.put(msg.getTransactionId(), status);
      return LocalTransactionState.UNKNOW;
  }
  @Override
  public LocalTransactionState checkLocalTransaction(MessageExt msg) {
      Integer status = localTrans.get(msg.getTransactionId());
      if (null != status) {
          switch (status) {
              case 0:
                  return LocalTransactionState.UNKNOW;
              case 1:
                  return LocalTransactionState.COMMIT_MESSAGE;
              case 2:
                  return LocalTransactionState.ROLLBACK_MESSAGE;
          }
      }
      return LocalTransactionState.COMMIT_MESSAGE;
  }
}
```

## 3 使用限制
1. 事务信息没有schedule和batch支持。
2. 为了避免单条消息被检查太多次并导致half队列消息累积，我们默认将单条消息的检查次数限制为 15 次，但可以通过更改broker配置中的 ```transactionCheckMax``` 参数来修改此限制，如果一条消息已被检查 ```transactionCheckMax``` 次，broker将丢弃此消息，在默认情况下将同时打印错误日志。用户可以通过重写```AbstractTransactionalMessageCheckListener``` 来更改此行为。
3. 事务消息将在一定时间后进行检查，该时间由broker配置中的```transactionTimeout``` 参数决定。用户还可以在发送事务消息时通过设置的用户属性 ```CHECK_IMMUNITY_TIME_IN_SECONDS``` 更改此限制，此参数优先于```transactionTimeout``` 参数。 
4. 事务性消息可能被多次检查或消费。
5. 已提交消息重发给用户目标topic可能会失败。当前，这取决于日志记录。RocketMQ本身的高可用性机制确保了高可用性。如果您要确保事务性消息不会丢失，并且事务完整性得到保证，建议使用同步双写机制。
6. 事务消息的生产者 ID 不能与其他类型的消息的生产者 ID 共享。与其他类型的消息不同，事务性消息允许后向查询。MQ服务器按其生产者ID查询客户端。

