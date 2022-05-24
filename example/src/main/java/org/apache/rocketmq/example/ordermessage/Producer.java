/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.ordermessage;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class Producer {
    public static void main(String[] args) throws UnsupportedEncodingException {
        try {
            DefaultMQProducer producer = new DefaultMQProducer("order_group");
            producer.setNamesrvAddr("127.0.0.1:9876");
            producer.start();
            List<OrderEntity> list = buildOrderList();
            for (int i = 0; i < list.size(); i++) {
                int orderId = list.get(i).getId();
                //Create a message instance, specifying topic, tag and message body.
                Message msg = new Message("orderTopic", "TagA", "KEY" + i,
                        (list.get(i).toString()).getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        Integer id = (Integer) arg;
                        int index = id % mqs.size();
                        return mqs.get(index);
                    }
                }, orderId);

                System.out.println("订单id:" + orderId + "  发送结果:" + sendResult);
            }
            //关闭生产者
            producer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<OrderEntity> buildOrderList() {
        List<OrderEntity> res = new ArrayList<>();

        OrderEntity order1 = new OrderEntity(147, "加入购物车");
        OrderEntity order2 = new OrderEntity(147, "下单");
        OrderEntity order3 = new OrderEntity(147, "付款");
        OrderEntity order4 = new OrderEntity(147, "完成");

        OrderEntity order5 = new OrderEntity(258, "加入购物车");
        OrderEntity order6 = new OrderEntity(258, "下单");

        OrderEntity order7 = new OrderEntity(369, "加入购物车");
        OrderEntity order8 = new OrderEntity(369, "下单");
        OrderEntity order9 = new OrderEntity(369, "付款");
        res.add(order1);
        res.add(order2);
        res.add(order3);
        res.add(order4);
        res.add(order5);
        res.add(order6);
        res.add(order7);
        res.add(order8);
        res.add(order9);
        return res;
    }

    static class OrderEntity {
        int id;
        String msg;

        public OrderEntity(int id, String msg) {
            this.id = id;
            this.msg = msg;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }

        @Override
        public String toString() {
            return "OrderEntity{" +
                    "id=" + id +
                    ", msg='" + msg + '\'' +
                    '}';
        }
    }

}
