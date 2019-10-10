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
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class RemoteAddressStrategyFactory {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    public static final NullRemoteAddressStrategy NULL_NET_ADDRESS_STRATEGY = new NullRemoteAddressStrategy();

    public static final BlankRemoteAddressStrategy BLANK_NET_ADDRESS_STRATEGY = new BlankRemoteAddressStrategy();

    public RemoteAddressStrategy getRemoteAddressStrategy(PlainAccessResource plainAccessResource) {
        return getRemoteAddressStrategy(plainAccessResource.getWhiteRemoteAddress());
    }

    public RemoteAddressStrategy getRemoteAddressStrategy(String remoteAddr) {
        if (StringUtils.isBlank(remoteAddr)) {
            return BLANK_NET_ADDRESS_STRATEGY;
        }
        if ("*".equals(remoteAddr) || "*.*.*.*".equals(remoteAddr)) {
            return NULL_NET_ADDRESS_STRATEGY;
        }
        if (remoteAddr.endsWith("}")) {
            String[] strArray = StringUtils.split(remoteAddr, ".");
            String four = strArray[3];
            if (!four.startsWith("{")) {
                throw new AclException(String.format("MultipleRemoteAddressStrategy netaddress examine scope Exception netaddress", remoteAddr));
            }
            return new MultipleRemoteAddressStrategy(AclUtils.getAddreeStrArray(remoteAddr, four));
        } else if (AclUtils.isColon(remoteAddr)) {
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
            for (String netaddress : strArray) {
                AclUtils.verify(netaddress, 4);
                multipleSet.add(netaddress);
            }
        }

        @Override
        public boolean match(PlainAccessResource plainAccessResource) {
            return multipleSet.contains(plainAccessResource.getWhiteRemoteAddress());
        }

    }

    public static class OneRemoteAddressStrategy implements RemoteAddressStrategy {

        private String netaddress;

        public OneRemoteAddressStrategy(String netaddress) {
            this.netaddress = netaddress;
            AclUtils.verify(netaddress, 4);
        }

        @Override
        public boolean match(PlainAccessResource plainAccessResource) {
            return netaddress.equals(plainAccessResource.getWhiteRemoteAddress());
        }

    }

    public static class RangeRemoteAddressStrategy implements RemoteAddressStrategy {

        private String head;

        private int start;

        private int end;

        private int index;

        public RangeRemoteAddressStrategy(String remoteAddr) {
            String[] strArray = StringUtils.split(remoteAddr, ".");
            if (analysis(strArray, 1) || analysis(strArray, 2) || analysis(strArray, 3)) {
                AclUtils.verify(remoteAddr, index - 1);
                StringBuffer sb = new StringBuffer().append(strArray[0].trim()).append(".").append(strArray[1].trim()).append(".");
                if (index == 3) {
                    sb.append(strArray[2].trim()).append(".");
                }
                this.head = sb.toString();
            }
        }

        private boolean analysis(String[] strArray, int index) {
            String value = strArray[index].trim();
            this.index = index;
            if ("*".equals(value)) {
                setValue(0, 255);
            } else if (AclUtils.isMinus(value)) {
                if (value.indexOf("-") == 0) {
                    throw new AclException(String.format("RangeRemoteAddressStrategy netaddress examine scope Exception value %s ", value));

                }
                String[] valueArray = StringUtils.split(value, "-");
                this.start = Integer.valueOf(valueArray[0]);
                this.end = Integer.valueOf(valueArray[1]);
                if (!(AclUtils.isScope(end) && AclUtils.isScope(start) && start <= end)) {
                    throw new AclException(String.format("RangeRemoteAddressStrategy netaddress examine scope Exception start is %s , end is %s", start, end));
                }
            }
            return this.end > 0 ? true : false;
        }

        private void setValue(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public boolean match(PlainAccessResource plainAccessResource) {
            String netAddress = plainAccessResource.getWhiteRemoteAddress();
            if (netAddress.startsWith(this.head)) {
                String value;
                if (index == 3) {
                    value = netAddress.substring(this.head.length());
                } else {
                    value = netAddress.substring(this.head.length(), netAddress.lastIndexOf('.'));
                }
                Integer address = Integer.valueOf(value);
                if (address >= this.start && address <= this.end) {
                    return true;
                }
            }
            return false;
        }

    }

}
