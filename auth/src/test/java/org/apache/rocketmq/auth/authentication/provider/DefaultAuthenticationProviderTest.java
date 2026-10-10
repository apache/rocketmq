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
package org.apache.rocketmq.auth.authentication.provider;

import org.junit.Assert;
import org.junit.Test;

public class DefaultAuthenticationProviderTest {

    @Test
    public void maskSignatureShouldRedactShortSignatures() {
        Assert.assertEquals("****", DefaultAuthenticationProvider.maskSignature("test"));
        Assert.assertEquals("****", DefaultAuthenticationProvider.maskSignature("abcd1234"));
        Assert.assertEquals("****", DefaultAuthenticationProvider.maskSignature(null));
    }

    @Test
    public void maskSignatureShouldKeepOnlyPrefixAndSuffix() {
        String signature = "DJRRXBXlCVuKh6ULoN87847QX+Y=";

        String masked = DefaultAuthenticationProvider.maskSignature(signature);

        Assert.assertEquals("DJRR****X+Y=", masked);
        Assert.assertFalse(masked.contains("XBXlCVuKh6ULoN87847Q"));
    }
}
