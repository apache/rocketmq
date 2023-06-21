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
package org.apache.rocketmq.acl.common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Map;
import java.util.SortedMap;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.yaml.snakeyaml.Yaml;

import static org.apache.rocketmq.acl.common.SessionCredentials.CHARSET;

public class AclUtils {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    public static byte[] combineRequestContent(RemotingCommand request, SortedMap<String, String> fieldsMap) {
        try {
            StringBuilder sb = new StringBuilder("");
            for (Map.Entry<String, String> entry : fieldsMap.entrySet()) {
                if (!SessionCredentials.SIGNATURE.equals(entry.getKey())) {
                    sb.append(entry.getValue());
                }
            }

            return AclUtils.combineBytes(sb.toString().getBytes(CHARSET), request.getBody());
        } catch (Exception e) {
            throw new RuntimeException("Incompatible exception.", e);
        }
    }

    public static byte[] combineBytes(byte[] b1, byte[] b2) {
        if (b1 == null || b1.length == 0) return b2;
        if (b2 == null || b2.length == 0) return b1;
        byte[] total = new byte[b1.length + b2.length];
        System.arraycopy(b1, 0, total, 0, b1.length);
        System.arraycopy(b2, 0, total, b1.length, b2.length);
        return total;
    }

    public static String calSignature(byte[] data, String secretKey) {
        String signature = AclSigner.calSignature(data, secretKey);
        return signature;
    }

    public static void IPv6AddressCheck(String netAddress) {
        if (isAsterisk(netAddress) || isMinus(netAddress)) {
            int asterisk = netAddress.indexOf("*");
            int minus = netAddress.indexOf("-");
//            '*' must be the end of netAddress if it exists
            if (asterisk > -1 && asterisk != netAddress.length() - 1) {
                throw new AclException(String.format("NetAddress examine scope Exception netAddress is %s", netAddress));
            }

//            format like "2::ac5:78:1-200:*" or "2::ac5:78:1-200" is legal
            if (minus > -1) {
                if (asterisk == -1) {
                    if (minus <= netAddress.lastIndexOf(":")) {
                        throw new AclException(String.format("NetAddress examine scope Exception netAddress is %s", netAddress));
                    }
                } else {
                    if (minus <= netAddress.lastIndexOf(":", netAddress.lastIndexOf(":") - 1)) {
                        throw new AclException(String.format("NetAddress examine scope Exception netAddress is %s", netAddress));
                    }
                }
            }
        }
    }

    public static String v6ipProcess(String netAddress) {
        int part;
        String subAddress;
        boolean isAsterisk = isAsterisk(netAddress);
        boolean isMinus = isMinus(netAddress);
        if (isAsterisk && isMinus) {
            part = 6;
            int lastColon = netAddress.lastIndexOf(':');
            int secondLastColon = netAddress.substring(0, lastColon).lastIndexOf(':');
            subAddress = netAddress.substring(0, secondLastColon);
        } else if (!isAsterisk && !isMinus) {
            part = 8;
            subAddress = netAddress;
        } else {
            part = 7;
            subAddress = netAddress.substring(0, netAddress.lastIndexOf(':'));
        }
        return expandIP(subAddress, part);
    }

    public static void verify(String netAddress, int index) {
        if (!AclUtils.isScope(netAddress, index)) {
            throw new AclException(String.format("NetAddress examine scope Exception netAddress is %s", netAddress));
        }
    }

    public static String[] getAddresses(String netAddress, String partialAddress) {
        String[] parAddStrArray = StringUtils.split(partialAddress.substring(1, partialAddress.length() - 1), ",");
        String address = netAddress.substring(0, netAddress.indexOf("{"));
        String[] addressStrArray = new String[parAddStrArray.length];
        for (int i = 0; i < parAddStrArray.length; i++) {
            addressStrArray[i] = address + parAddStrArray[i];
        }
        return addressStrArray;
    }

    public static boolean isScope(String netAddress, int index) {
//        IPv6 Address
        if (isColon(netAddress)) {
            netAddress = expandIP(netAddress, 8);
            String[] strArray = StringUtils.split(netAddress, ":");
            return isIPv6Scope(strArray, index);
        }

        String[] strArray = StringUtils.split(netAddress, ".");
        if (strArray.length != 4) {
            return false;
        }
        return isScope(strArray, index);

    }

    public static boolean isScope(String[] num, int index) {
        for (int i = 0; i < index; i++) {
            if (!isScope(num[i])) {
                return false;
            }
        }
        return true;
    }

    public static boolean isColon(String netAddress) {
        return netAddress.indexOf(':') > -1;
    }

    public static boolean isScope(String num) {
        return isScope(Integer.parseInt(num.trim()));
    }

    public static boolean isScope(int num) {
        return num >= 0 && num <= 255;
    }

    public static boolean isAsterisk(String asterisk) {
        return asterisk.indexOf('*') > -1;
    }

    public static boolean isComma(String colon) {
        return colon.indexOf(',') > -1;
    }

    public static boolean isMinus(String minus) {
        return minus.indexOf('-') > -1;

    }

    public static boolean isIPv6Scope(String[] num, int index) {
        for (int i = 0; i < index; i++) {
            int value;
            try {
                value = Integer.parseInt(num[i], 16);
            } catch (NumberFormatException e) {
                return false;
            }
            if (!isIPv6Scope(value)) {
                return false;
            }
        }
        return true;
    }

    public static boolean isIPv6Scope(int num) {
        int min = Integer.parseInt("0", 16);
        int max = Integer.parseInt("ffff", 16);
        return num >= min && num <= max;
    }

    public static String expandIP(String netAddress, int part) {
        netAddress = netAddress.toUpperCase();
        // expand netAddress
        int separatorCount = StringUtils.countMatches(netAddress, ":");
        int padCount = part - separatorCount;
        if (padCount > 0) {
            StringBuilder padStr = new StringBuilder(":");
            for (int i = 0; i < padCount; i++) {
                padStr.append(":");
            }
            netAddress = StringUtils.replace(netAddress, "::", padStr.toString());
        }

        // pad netAddress
        String[] strArray = StringUtils.splitPreserveAllTokens(netAddress, ":");
        for (int i = 0; i < strArray.length; i++) {
            if (strArray[i].length() < 4) {
                strArray[i] = StringUtils.leftPad(strArray[i], 4, '0');
            }
        }

        // output
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < strArray.length; i++) {
            sb.append(strArray[i]);
            if (i != strArray.length - 1) {
                sb.append(":");
            }
        }
        return sb.toString();
    }

    public static <T> T getYamlDataObject(String path, Class<T> clazz) {
        try (FileInputStream fis = new FileInputStream(path)) {
            return getYamlDataObject(fis, clazz);
        } catch (FileNotFoundException ignore) {
            return null;
        } catch (Exception e) {
            throw new AclException(e.getMessage(), e);
        }
    }

    public static <T> T getYamlDataObject(InputStream fis, Class<T> clazz) {
        Yaml yaml = new Yaml();
        try {
            return yaml.loadAs(fis, clazz);
        } catch (Exception e) {
            throw new AclException(e.getMessage(), e);
        }
    }

    public static boolean writeDataObject(String path, Object dataMap) {
        Yaml yaml = new Yaml();
        try (PrintWriter pw = new PrintWriter(path, "UTF-8")) {
            String dumpAsMap = yaml.dumpAsMap(dataMap);
            pw.print(dumpAsMap);
            pw.flush();
        } catch (Exception e) {
            throw new AclException(e.getMessage(), e);
        }
        return true;
    }

    public static RPCHook getAclRPCHook(String fileName) {
        JSONObject yamlDataObject;
        try {
            yamlDataObject = AclUtils.getYamlDataObject(fileName,
                JSONObject.class);
        } catch (Exception e) {
            log.error("Convert yaml file to data object error, ", e);
            return null;
        }
        return buildRpcHook(yamlDataObject);
    }

    public static RPCHook getAclRPCHook(InputStream inputStream) {
        JSONObject yamlDataObject = null;
        try {
            yamlDataObject = AclUtils.getYamlDataObject(inputStream, JSONObject.class);
        } catch (Exception e) {
            log.error("Convert yaml file to data object error, ", e);
            return null;
        }
        return buildRpcHook(yamlDataObject);
    }

    private static RPCHook buildRpcHook(JSONObject yamlDataObject) {
        if (yamlDataObject == null || yamlDataObject.isEmpty()) {
            log.warn("Failed to parse configuration to enable ACL.");
            return null;
        }

        String accessKey = yamlDataObject.getString(AclConstants.CONFIG_ACCESS_KEY);
        String secretKey = yamlDataObject.getString(AclConstants.CONFIG_SECRET_KEY);

        if (StringUtils.isBlank(accessKey) || StringUtils.isBlank(secretKey)) {
            log.warn("Failed to enable ACL. Either AccessKey or secretKey is blank");
            return null;
        }
        return new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
    }

}
