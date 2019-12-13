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
package org.apache.rocketmq.ons.api;

import io.openmessaging.api.Consumer;
import io.openmessaging.api.OMS;
import io.openmessaging.api.Producer;
import io.openmessaging.api.PullConsumer;
import io.openmessaging.api.batch.BatchConsumer;
import io.openmessaging.api.order.OrderConsumer;
import io.openmessaging.api.order.OrderProducer;
import io.openmessaging.api.transaction.LocalTransactionChecker;
import io.openmessaging.api.transaction.TransactionProducer;
import java.util.Properties;

/**
 * {@link OMS} is recommended.
 */
@Deprecated
public class ONSFactory {

    private static ONSFactoryAPI onsFactory = null;

    static {

        try {
            Class<?> factoryClass =
                ONSFactory.class.getClassLoader().loadClass(
                    "org.apache.rocketmq.ons.api.impl.ONSFactoryImpl");
            onsFactory = (ONSFactoryAPI) factoryClass.newInstance();
        } catch (Throwable e) {
            e.printStackTrace();
        }

    }

    /**
     * Create Producer
     *
     * <p>
     * <code>properties</code>
     * Require:
     * <ol>
     * <li>{@link PropertyKeyConst#GROUP_ID}</li>
     * <li>{@link PropertyKeyConst#AccessKey}</li>
     * <li>{@link PropertyKeyConst#SecretKey}</li>
     * <li>{@link PropertyKeyConst#ONSAddr}</li>
     * </ol>
     * Optional:
     * <ol>
     * <li>{@link PropertyKeyConst#OnsChannel}</li>
     * <li>{@link PropertyKeyConst#SendMsgTimeoutMillis}</li>
     * <li>{@link PropertyKeyConst#NAMESRV_ADDR} will override {@link PropertyKeyConst#ONSAddr}</li>
     * </ol>
     * </p>
     *
     *
     * <p>
     * sample:
     * <pre>
     *        Properties props = ...;
     *        Producer producer = ONSFactory.createProducer(props);
     *        producer.start();
     *
     *
     *        Message msg = ...;
     *        SendResult result = producer.send(msg);
     *
     *        producer.shutdown();
     *   </pre>
     * </p>
     *
     * @param properties Producer's configuration
     * @return {@link Producer}  Thread safe {@link Producer} instance
     */
    public static Producer createProducer(final Properties properties) {
        return onsFactory.createProducer(properties);
    }

    /**
     * Create OrderProducer
     * <p>
     * <code>properties</code>
     * Require:
     * <ol>
     * <li>{@link PropertyKeyConst#GROUP_ID}</li>
     * <li>{@link PropertyKeyConst#AccessKey}</li>
     * <li>{@link PropertyKeyConst#SecretKey}</li>
     * <li>{@link PropertyKeyConst#ONSAddr}</li>
     * </ol>
     * Optional:
     * <ul>
     * <li>{@link PropertyKeyConst#NAMESRV_ADDR}</li>
     * <li>{@link PropertyKeyConst#OnsChannel}</li>
     * <li>{@link PropertyKeyConst#SendMsgTimeoutMillis}</li>
     * </ul>
     * </p>
     *
     * @param properties Producer configuration
     * @return {@code OrderProducer} Thread safe {@link OrderProducer} instance
     */
    public static OrderProducer createOrderProducer(final Properties properties) {
        return onsFactory.createOrderProducer(properties);
    }

    /**
     * Create Transaction Producer
     * <p>
     * <code>properties</code>Requires:
     * <ol>
     * <li>{@link PropertyKeyConst#GROUP_ID}</li>
     * <li>{@link PropertyKeyConst#AccessKey}</li>
     * <li>{@link PropertyKeyConst#SecretKey}</li>
     * <li>{@link PropertyKeyConst#ONSAddr}</li>
     * </ol>
     * Optional:
     * <ul>
     * <li>{@link PropertyKeyConst#NAMESRV_ADDR}</li>
     * <li>{@link PropertyKeyConst#OnsChannel}</li>
     * <li>{@link PropertyKeyConst#SendMsgTimeoutMillis}</li>
     * <li>{@link PropertyKeyConst#CheckImmunityTimeInSeconds}</li>
     * </ul>
     * </p>
     *
     * @param properties Producer configuration
     * @return {@code TransactionProducer} Thread safe {@link TransactionProducer}  instance
     */
    public static TransactionProducer createTransactionProducer(final Properties properties,
        final LocalTransactionChecker checker) {
        return onsFactory.createTransactionProducer(properties, checker);
    }

    /**
     * Create Consumer
     * <p>
     * <code>properties</code>
     * Requires:
     * <ol>
     * <li>{@link PropertyKeyConst#GROUP_ID}</li>
     * <li>{@link PropertyKeyConst#AccessKey}</li>
     * <li>{@link PropertyKeyConst#SecretKey}</li>
     * <li>{@link PropertyKeyConst#ONSAddr}</li>
     * </ol>
     * Optional:
     * <ul>
     * <li>{@link PropertyKeyConst#ConsumeThreadNums}</li>
     * <li>{@link PropertyKeyConst#ConsumeTimeout}</li>
     * <li>{@link PropertyKeyConst#OnsChannel}</li>
     * </ul>
     * </p>
     *
     * @param properties Consumer's configuration
     * @return {@code Consumer} Thread safe {@link Consumer} instance
     */
    public static Consumer createConsumer(final Properties properties) {
        return onsFactory.createConsumer(properties);
    }

    /**
     * Create BatchConsumer
     * <p>
     * <code>properties</code>
     * Requires:
     * <ol>
     * <li>{@link PropertyKeyConst#GROUP_ID}</li>
     * <li>{@link PropertyKeyConst#AccessKey}</li>
     * <li>{@link PropertyKeyConst#SecretKey}</li>
     * <li>{@link PropertyKeyConst#ONSAddr}</li>
     * </ol>
     * Optional:
     * <ul>
     * <li>{@link PropertyKeyConst#ConsumeThreadNums}</li>
     * <li>{@link PropertyKeyConst#ConsumeTimeout}</li>
     * <li>{@link PropertyKeyConst#ConsumeMessageBatchMaxSize}</li>
     * <li>{@link PropertyKeyConst#OnsChannel}</li>
     * </ul>
     * </p>
     *
     * @param properties BatchConsumer's configuration
     * @return {@code BatchConsumer} Thread safe {@link BatchConsumer} instance
     */
    public static BatchConsumer createBatchConsumer(final Properties properties) {
        return onsFactory.createBatchConsumer(properties);
    }

    /**
     * Create Order Consumer
     * <p>
     * <code>properties</code>
     * Requires:
     * <ol>
     * <li>{@link PropertyKeyConst#GROUP_ID}</li>
     * <li>{@link PropertyKeyConst#AccessKey}</li>
     * <li>{@link PropertyKeyConst#SecretKey}</li>
     * <li>{@link PropertyKeyConst#ONSAddr}</li>
     * </ol>
     * Optional:
     * <ul>
     * <li>{@link PropertyKeyConst#ConsumeThreadNums}</li>
     * <li>{@link PropertyKeyConst#ConsumeTimeout}</li>
     * <li>{@link PropertyKeyConst#OnsChannel}</li>
     * </ul>
     * </p>
     *
     * @param properties Consumer's configuration
     * @return {@code OrderConsumer} Thread safe {@link OrderConsumer} instance
     */
    public static OrderConsumer createOrderedConsumer(final Properties properties) {
        return onsFactory.createOrderedConsumer(properties);
    }

    /**
     * Create Order Consumer
     * <p>
     * <code>properties</code>
     * Requires:
     * <ol>
     * <li>{@link PropertyKeyConst#GROUP_ID}</li>
     * <li>{@link PropertyKeyConst#AccessKey}</li>
     * <li>{@link PropertyKeyConst#SecretKey}</li>
     * <li>{@link PropertyKeyConst#ONSAddr}</li>
     * </ol>
     * Optional:
     * <ul>
     * <li>{@link PropertyKeyConst#ConsumeThreadNums}</li>
     * <li>{@link PropertyKeyConst#OnsChannel}</li>
     * </ul>
     * </p>
     *
     * @param properties Consumer's configuration
     * @return {@code PullConsumer} Thread safe {@link PullConsumer} instance
     */
    public static PullConsumer createPullConsumer(final Properties properties) {
        return onsFactory.createPullConsumer(properties);
    }

}
