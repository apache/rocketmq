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
package org.apache.rocketmq.ratelimit.model;

import org.apache.rocketmq.common.utils.IPAddressUtils;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.apache.rocketmq.common.constant.CommonConstants.SLASH;

public class CidrMatcher implements Matcher {
    private final BigInteger lower;
    private final BigInteger upper;

    public CidrMatcher(String cidr) {
        if (!IPAddressUtils.isValidIPOrCidr(cidr)) {
            throw new IllegalArgumentException(String.format("cidr %s is not valid", cidr));
        }
        String[] parts = cidr.split(SLASH);
        if (parts.length > 2) {
            throw new IllegalArgumentException(String.format("cidr %s is not valid", cidr));
        }

        InetAddress cidrIp;
        try {
            cidrIp = InetAddress.getByName(parts[0]);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(String.format("cidr %s is not valid", cidr));
        }
        BigInteger cidrIpBigInt = new BigInteger(1, cidrIp.getAddress());
        if (parts.length == 1) {
            lower = cidrIpBigInt;
            upper = cidrIpBigInt;
            return;
        }

        int prefixLength = Integer.parseInt(parts[1]);
        BigInteger mask = BigInteger.valueOf(-1).shiftLeft(cidrIp.getAddress().length * 8 - prefixLength);
        lower = cidrIpBigInt.and(mask);
        upper = lower.add(mask.not());
    }


    @Override
    public boolean matches(String ip) {
        if (ip == null) {
            return false;
        }
        if (!IPAddressUtils.isValidIp(ip)) {
            return false;
        }
        try {
            BigInteger ipBigInt = new BigInteger(1, InetAddress.getByName(ip).getAddress());
            return ipBigInt.compareTo(lower) >= 0 && ipBigInt.compareTo(upper) <= 0;
        } catch (UnknownHostException e) {
            return false;
        }
    }
}
