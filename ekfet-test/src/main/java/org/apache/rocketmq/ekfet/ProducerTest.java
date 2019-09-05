package org.apache.rocketmq.ekfet;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

public class ProducerTest {
    private static final DefaultMQProducer defaultMQProducer = new DefaultMQProducer("test_group");

    static {
        defaultMQProducer.setNamesrvAddr("127.0.0.1:7986");

    }

    public static void main(String[] args) {
        try {
            defaultMQProducer.start();
            for (int i = 0; i < 100; i++) {
                Message msg = new Message("TopicTest",
                        "TagA",
                        "OrderID188",
                        "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = defaultMQProducer.send(msg);
                System.out.printf("%s%n", sendResult);

            }
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } finally {
            defaultMQProducer.shutdown();
        }
    }

}
