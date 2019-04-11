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

import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.yaml.snakeyaml.Yaml;

import static org.apache.rocketmq.acl.common.SessionCredentials.CHARSET;

public class AclUtils {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

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
            throw new RuntimeException("incompatible exception.", e);
        }
    }

    public static byte[] combineBytes(byte[] b1, byte[] b2) {
        int size = (null != b1 ? b1.length : 0) + (null != b2 ? b2.length : 0);
        byte[] total = new byte[size];
        if (null != b1)
            System.arraycopy(b1, 0, total, 0, b1.length);
        if (null != b2)
            System.arraycopy(b2, 0, total, b1.length, b2.length);
        return total;
    }

    public static String calSignature(byte[] data, String secretKey) {
        String signature = AclSigner.calSignature(data, secretKey);
        return signature;
    }

    public static void verify(String netaddress, int index) {
        if (!AclUtils.isScope(netaddress, index)) {
            throw new AclException(String.format("netaddress examine scope Exception netaddress is %s", netaddress));
        }
    }

    public static String[] getAddreeStrArray(String netaddress, String four) {
        String[] fourStrArray = StringUtils.split(four.substring(1, four.length() - 1), ",");
        String address = netaddress.substring(0, netaddress.indexOf("{"));
        String[] addreeStrArray = new String[fourStrArray.length];
        for (int i = 0; i < fourStrArray.length; i++) {
            addreeStrArray[i] = address + fourStrArray[i];
        }
        return addreeStrArray;
    }

    public static boolean isScope(String num, int index) {
        String[] strArray = StringUtils.split(num, ".");
        if (strArray.length != 4) {
            return false;
        }
        return isScope(strArray, index);

    }

    public static boolean isScope(String[] num, int index) {
        if (num.length <= index) {

        }
        for (int i = 0; i < index; i++) {
            if (!isScope(num[i])) {
                return false;
            }
        }
        return true;

    }

    public static boolean isScope(String num) {
        return isScope(Integer.valueOf(num.trim()));
    }

    public static boolean isScope(int num) {
        return num >= 0 && num <= 255;
    }

    public static boolean isAsterisk(String asterisk) {
        return asterisk.indexOf('*') > -1;
    }

    public static boolean isColon(String colon) {
        return colon.indexOf(',') > -1;
    }

    public static boolean isMinus(String minus) {
        return minus.indexOf('-') > -1;

    }

    public static <T> T getYamlDataObject(String path, Class<T> clazz) {
        Yaml ymal = new Yaml();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(new File(path));
            return ymal.loadAs(fis, clazz);
        } catch (FileNotFoundException ignore) {
            return null;
        } catch (Exception e) {
            throw new AclException(e.getMessage());
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ignore) {
                }
            }
        }
    }

    public static RPCHook getAclRPCHook(String fileName) {
        JSONObject yamlDataObject = null;
        try {
            yamlDataObject = AclUtils.getYamlDataObject(fileName,
                JSONObject.class);
        } catch (Exception e) {
            log.error("convert yaml file to data object error, ",e);
            return null;
        }

        if (yamlDataObject == null || yamlDataObject.isEmpty()) {
            log.warn("Cannot find conf file :{}, acl isn't be enabled." ,fileName);
            return null;
        }
        
        String accessKey = yamlDataObject.getString("accessKey");
        String secretKey = yamlDataObject.getString("secretKey");

        if (StringUtils.isBlank(accessKey) || StringUtils.isBlank(secretKey)) {
            log.warn("AccessKey or secretKey is blank, the acl is not enabled.");

            return null;
        }
        return new AclClientRPCHook(new SessionCredentials(accessKey,secretKey));
    }

}
