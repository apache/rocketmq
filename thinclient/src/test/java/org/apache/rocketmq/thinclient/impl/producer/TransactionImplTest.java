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

package org.apache.rocketmq.thinclient.impl.producer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.apis.ClientException;
import org.apache.rocketmq.apis.message.Message;
import org.apache.rocketmq.apis.message.MessageId;
import org.apache.rocketmq.apis.producer.TransactionResolution;
import org.apache.rocketmq.thinclient.message.MessageCommon;
import org.apache.rocketmq.thinclient.message.PublishingMessageImpl;
import org.apache.rocketmq.thinclient.route.Endpoints;
import org.apache.rocketmq.thinclient.tool.TestBase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TransactionImplTest extends TestBase {
    @Mock
    ProducerImpl producer;

    @Test
    public void testTryAddMessage() throws IOException, NoSuchFieldException, IllegalAccessException {
        final TransactionImpl transaction = new TransactionImpl(producer);
        final Message message0 = fakeMessage(FAKE_TOPIC_0);

        final Class<? extends ProducerImpl> clazz = producer.getClass();
        final Field field = clazz.getDeclaredField("producerSettings");
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(producer, fakeProducerSettings());

        transaction.tryAddMessage(message0);
        // Expect no exception thrown.
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTryAddMultipleMessages() throws IOException, NoSuchFieldException, IllegalAccessException {
        final TransactionImpl transaction = new TransactionImpl(producer);
        final Message message0 = fakeMessage(FAKE_TOPIC_0);
        final Message message1 = fakeMessage(FAKE_TOPIC_0);

        final Class<? extends ProducerImpl> clazz = producer.getClass();
        final Field field = clazz.getDeclaredField("producerSettings");
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(producer, fakeProducerSettings());

        transaction.tryAddMessage(message0);
        transaction.tryAddMessage(message1);
        // Expect no exception thrown.
    }

    @Test(expected = IllegalStateException.class)
    public void testCommitWithNoReceipts() throws ClientException {
        final TransactionImpl transaction = new TransactionImpl(producer);
        transaction.commit();
    }

    @Test(expected = IllegalStateException.class)
    public void testRollbackWithNoReceipts() throws ClientException {
        final TransactionImpl transaction = new TransactionImpl(producer);
        transaction.rollback();
    }

    @Test
    public void testCommit() throws IOException, ClientException, ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        final TransactionImpl transaction = new TransactionImpl(producer);
        final Message message0 = fakeMessage(FAKE_TOPIC_0);

        final Class<? extends ProducerImpl> clazz = producer.getClass();
        final Field field = clazz.getDeclaredField("producerSettings");
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(producer, fakeProducerSettings());

        final PublishingMessageImpl publishingMessage = transaction.tryAddMessage(message0);
        final SendReceiptImpl receipt = fakeSendReceiptImpl(fakeMessageQueueImpl0());
        transaction.tryAddReceipt(publishingMessage, receipt);
        ArgumentCaptor<TransactionResolution> resolutionArgumentCaptor = ArgumentCaptor.forClass(TransactionResolution.class);
        doNothing().when(producer).endTransaction(any(Endpoints.class), any(MessageCommon.class), any(MessageId.class), anyString(), resolutionArgumentCaptor.capture());
        transaction.commit();
        assertEquals(TransactionResolution.COMMIT, resolutionArgumentCaptor.getValue());
    }

    @Test
    public void testRollback() throws IOException, ClientException, ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        final TransactionImpl transaction = new TransactionImpl(producer);
        final Message message0 = fakeMessage(FAKE_TOPIC_0);

        final Class<? extends ProducerImpl> clazz = producer.getClass();
        final Field field = clazz.getDeclaredField("producerSettings");
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(producer, fakeProducerSettings());

        final PublishingMessageImpl publishingMessage = transaction.tryAddMessage(message0);
        final SendReceiptImpl receipt = fakeSendReceiptImpl(fakeMessageQueueImpl0());
        transaction.tryAddReceipt(publishingMessage, receipt);
        ArgumentCaptor<TransactionResolution> resolutionArgumentCaptor = ArgumentCaptor.forClass(TransactionResolution.class);
        doNothing().when(producer).endTransaction(any(Endpoints.class), any(MessageCommon.class), any(MessageId.class), anyString(), resolutionArgumentCaptor.capture());
        transaction.rollback();
        assertEquals(TransactionResolution.ROLLBACK, resolutionArgumentCaptor.getValue());
    }
}