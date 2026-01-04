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
package org.apache.rocketmq.tieredstore.exception;

import org.junit.Assert;
import org.junit.Test;

public class TieredStoreExceptionTest {

    @Test
    public void testMessageStoreException() {
        long position = 100L;
        String requestId = "requestId";
        String error = "ErrorMessage";

        TieredStoreException tieredStoreException = new TieredStoreException(TieredStoreErrorCode.IO_ERROR, error);
        Assert.assertEquals(TieredStoreErrorCode.IO_ERROR, tieredStoreException.getErrorCode());
        Assert.assertEquals(error, tieredStoreException.getMessage());

        tieredStoreException.setRequestId(requestId);
        Assert.assertEquals(requestId, tieredStoreException.getRequestId());

        tieredStoreException.setPosition(position);
        Assert.assertEquals(position, tieredStoreException.getPosition());
        Assert.assertNotNull(tieredStoreException.toString());
    }
}