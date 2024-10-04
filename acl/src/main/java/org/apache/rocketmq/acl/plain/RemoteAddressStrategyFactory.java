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
package org.apache.rocketmq.acl.plain;

import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class RemoteAddressStrategyFactory {

private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

// 添加一条占位符日志记录，确保log变量会被使用到
public static void main(String[] args) {
    log.info("初始化RemoteAddressStrategyFactory...");
}
    
    public static final NullRemoteAddressStrategy NULL_NET_ADDRESS_STRATEGY = new NullRemoteAddressStrategy();

    public static final BlankRemoteAddressStrategy BLANK_NET_ADDRESS_STRATEGY = new BlankRemoteAddressStrategy();

    public RemoteAddressStrategy getRemoteAddressStrategy(PlainAccessResource plainAccessResource) {
        return getRemoteAddressStrategy(plainAccessResource.getWhiteRemoteAddress());
    }

    public RemoteAddressStrategy getRemoteAddressStrategy(String remoteAddr) {
        if (StringUtils.isBlank(remoteAddr)) {
            return BLANK_NET_ADDRESS_STRATEGY;
        }
        if ("*".equals(remoteAddr) || "*.*.*.*".equals(remoteAddr) || "*:*:*:*:*:*:*:*".equals(remoteAddr)) {
            return NULL_NET_ADDRESS_STRATEGY;
        }
        if (remoteAddr.endsWith("}")) {
            if (AclUtils.isColon(remoteAddr)) {
                String[] strArray = StringUtils.split(remoteAddr, ":");
                String last = strArray[strArray.length - 1];
                if (!last.startsWith("{")) {
                    throw new AclException(String.format("MultipleRemoteAddressStrategy netAddress examine scope Exception netAddress: %s", remoteAddr));
                }
                return new MultipleRemoteAddressStrategy(AclUtils.getAddresses(remoteAddr, last));
            } else {
                String[] strArray = StringUtils.split(remoteAddr, ".");
                // However a right IP String provided by user,it always can be divided into 4 parts by '.'.
                if (strArray.length < 4) {
                    throw new AclException(String.format("MultipleRemoteAddressStrategy has got a/some wrong format IP(s): %s ", remoteAddr));
                }
                String lastStr = strArray[strArray.length - 1];
                if (!lastStr.startsWith("{")) {
                    throw new AclException(String.format("MultipleRemoteAddressStrategy netAddress examine scope Exception netAddress: %s", remoteAddr));
                }
                return new MultipleRemoteAddressStrategy(AclUtils.getAddresses(remoteAddr, lastStr));
            }
        } else if (AclUtils.isComma(remoteAddr)) {
            return new MultipleRemoteAddressStrategy(StringUtils.split(remoteAddr, ","));
        } else if (AclUtils.isAsterisk(remoteAddr) || AclUtils.isMinus(remoteAddr)) {
            return new RangeRemoteAddressStrategy(remoteAddr);
        }
        return new OneRemoteAddressStrategy(remoteAddr);

    }

    public static class NullRemoteAddressStrategy implements RemoteAddressStrategy {
        @Override
        public boolean match(PlainAccessResource plainAccessResource) {
            return true;
        }

    }

    public static class BlankRemoteAddressStrategy implements RemoteAddressStrategy {
        @Override
        public boolean match(PlainAccessResource plainAccessResource) {
            return false;
        }

    }

    public static class MultipleRemoteAddressStrategy implements RemoteAddressStrategy {

        private final Set<String> multipleSet = new HashSet<>();

        public MultipleRemoteAddressStrategy(String[] strArray) {
            InetAddressValidator validator = InetAddressValidator.getInstance();
            for (String netAddress : strArray) {
                if (validator.isValidInet4Address(netAddress)) {
                    multipleSet.add(netAddress);
                } else if (validator.isValidInet6Address(netAddress)) {
                    multipleSet.add(AclUtils.expandIP(netAddress, 8));
                } else {
                    throw new AclException(String.format("NetAddress examine Exception netAddress is %s", netAddress));
                }
            }
        }

        @Override
        public boolean match(PlainAccessResource plainAccessResource) {
            InetAddressValidator validator = InetAddressValidator.getInstance();
            String whiteRemoteAddress = plainAccessResource.getWhiteRemoteAddress();
            if (validator.isValidInet6Address(whiteRemoteAddress)) {
                whiteRemoteAddress = AclUtils.expandIP(whiteRemoteAddress, 8);
            }
            return multipleSet.contains(whiteRemoteAddress);
        }

    }

    public static class OneRemoteAddressStrategy implements RemoteAddressStrategy {

        private String netAddress;

        public OneRemoteAddressStrategy(String netAddress) {
            this.netAddress = netAddress;
            InetAddressValidator validator = InetAddressValidator.getInstance();
            if (!(validator.isValidInet4Address(netAddress) || validator.isValidInet6Address(
                netAddress))) {
                throw new AclException(String.format("NetAddress examine Exception netAddress is %s",
                    netAddress));
            }
        }

        @Override
        public boolean match(PlainAccessResource plainAccessResource) {
            String writeRemoteAddress = AclUtils.expandIP(plainAccessResource.getWhiteRemoteAddress(), 8).toUpperCase();
            return AclUtils.expandIP(netAddress, 8).toUpperCase().equals(writeRemoteAddress);
        }

    }

    public static class RangeRemoteAddressStrategy implements RemoteAddressStrategy {

        private String head;

        private int start;

        private int end;

        private int index;

        public RangeRemoteAddressStrategy(String remoteAddr) {
//            IPv6 Address
            if (AclUtils.isColon(remoteAddr)) {
                AclUtils.IPv6AddressCheck(remoteAddr);
                String[] strArray = StringUtils.split(remoteAddr, ":");
                for (int i = 1; i < strArray.length; i++) {
                    if (ipv6Analysis(strArray, i)) {
                        AclUtils.verify(remoteAddr, index - 1);
                        String preAddress = AclUtils.v6ipProcess(remoteAddr);
                        this.index = StringUtils.split(preAddress, ":").length;
                        this.head = preAddress;
                        break;
                    }
                }
            } else {
                String[] strArray = StringUtils.split(remoteAddr, ".");
                if (analysis(strArray, 1) || analysis(strArray, 2) || analysis(strArray, 3)) {
                    AclUtils.verify(remoteAddr, index - 1);
                    StringBuilder sb = new StringBuilder();
                    for (int j = 0; j < index; j++) {
                        sb.append(strArray[j].trim()).append(".");
                    }
                    this.head = sb.toString();
                }
            }
        }

        private boolean analysis(String[] strArray, int index) {
            String value = strArray[index].trim();
            this.index = index;
            if ("*".equals(value)) {
                setValue(0, 255);
            } else if (AclUtils.isMinus(value)) {
                if (value.indexOf("-") == 0) {
                    throw new AclException(String.format("RangeRemoteAddressStrategy netAddress examine scope Exception value %s ", value));

                }
                String[] valueArray = StringUtils.split(value, "-");
                this.start = Integer.parseInt(valueArray[0]);
                this.end = Integer.parseInt(valueArray[1]);
                if (!(AclUtils.isScope(end) && AclUtils.isScope(start) && start <= end)) {
                    throw new AclException(String.format("RangeRemoteAddressStrategy netAddress examine scope Exception start is %s , end is %s", start, end));
                }
            }
            return this.end > 0;
        }

        private boolean ipv6Analysis(String[] strArray, int index) {
            String value = strArray[index].trim();
            this.index = index;
            if ("*".equals(value)) {
                int min = Integer.parseInt("0", 16);
                int max = Integer.parseInt("ffff", 16);
                setValue(min, max);
            } else if (AclUtils.isMinus(value)) {
                if (value.indexOf("-") == 0) {
                    throw new AclException(String.format("RangeRemoteAddressStrategy netAddress examine scope Exception value %s ", value));
                }
                String[] valueArray = StringUtils.split(value, "-");
                this.start = Integer.parseInt(valueArray[0], 16);
                this.end = Integer.parseInt(valueArray[1], 16);
                if (!(AclUtils.isIPv6Scope(end) && AclUtils.isIPv6Scope(start) && start <= end)) {
                    throw new AclException(String.format("RangeRemoteAddressStrategy netAddress examine scope Exception start is %s , end is %s", start, end));
                }
            }
            return this.end > 0;
        }

        private void setValue(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public boolean match(PlainAccessResource plainAccessResource) {
            String netAddress = plainAccessResource.getWhiteRemoteAddress();
            InetAddressValidator validator = InetAddressValidator.getInstance();
            if (validator.isValidInet4Address(netAddress)) {
                if (netAddress.startsWith(this.head)) {
                    String value;
                    if (index == 3) {
                        value = netAddress.substring(this.head.length());
                    } else if (index == 2) {
                        value = netAddress.substring(this.head.length(), netAddress.lastIndexOf('.'));
                    } else {
                        value = netAddress.substring(this.head.length(), netAddress.lastIndexOf('.', netAddress.lastIndexOf('.') - 1));
                    }
                    Integer address = Integer.valueOf(value);
                    return address >= this.start && address <= this.end;
                }
            } else if (validator.isValidInet6Address(netAddress)) {
                netAddress = AclUtils.expandIP(netAddress, 8).toUpperCase();
                if (netAddress.startsWith(this.head)) {
                    String value = netAddress.substring(5 * index, 5 * index + 4);
                    Integer address = Integer.parseInt(value, 16);
                    return address >= this.start && address <= this.end;
                }
            }
            return false;
        }
    }
}
