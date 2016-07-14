### Filter网络架构，以CPU资源换取宝贵的网卡流量资源
![screenshot](http://img4.tbcdn.cn/L1/461/1/813c4f5a6934634233142e0a1cecb43507cbd1a5)

### 启动Broker时，增加以下配置，可以自动加载Filter Server进程

	filterServerNums=1

### Filter样本（Consumer仅负责将代码上传到Filter Server，由Filter Server编译后执行）

```java

package com.alibaba.rocketmq.example.filter;

import com.alibaba.rocketmq.common.filter.MessageFilter;
import com.alibaba.rocketmq.common.message.MessageExt;


public class MessageFilterImpl implements MessageFilter {

    @Override
    public boolean match(MessageExt msg) {
        String property = msg.getUserProperty("SequenceId");
        if (property != null) {
            int id = Integer.parseInt(property);
            if ((id % 3) == 0 && (id > 10)) {
                return true;
            }
        }

        return false;
    }
}


```


### Consumer例子

```java

    public static void main(String[] args) throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ConsumerGroupNamecc4");
        
        String filterCode = MixAll.file2String("/home/admin/MessageFilterImpl.java");
        consumer.subscribe("TopicFilter7", "com.alibaba.rocketmq.example.filter.MessageFilterImpl",
            filterCode);

        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                    ConsumeConcurrentlyContext context) {
                System.out.println(Thread.currentThread().getName() + " Receive New Messages: " + msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        <br>
        consumer.start();

        System.out.println("Consumer Started.");
    }


```




