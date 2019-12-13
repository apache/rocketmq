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

package org.apache.rocketmq.ons.api.impl.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class MsgConvertUtil {

    public static final byte[] EMPTY_BYTES = new byte[0];
    public static final String EMPTY_STRING = "";

    public static final String JMS_MSGMODEL = "jmsMsgModel";
    /**
     * To adapt this scene: "Notify client try to receive ObjectMessage sent by JMS client"
     * Set notify out message model, value can be textMessage OR objectMessage
     */
    public static final String COMPATIBLE_FIELD_MSGMODEL = "notifyOutMsgModel";

    public static final String MSGMODEL_TEXT = "textMessage";
    public static final String MSGMODEL_BYTES = "bytesMessage";
    public static final String MSGMODEL_OBJ = "objectMessage";

    public static final String MSG_TOPIC = "msgTopic";
    public static final String MSG_TYPE = "msgType";


    public static byte[] objectSerialize(Object object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(object);
        oos.close();
        baos.close();
        return baos.toByteArray();
    }

    public static Serializable objectDeserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        ois.close();
        bais.close();
        return (Serializable) ois.readObject();
    }

    public static final byte[] string2Bytes(String s, String charset) {
        if (null == s) {
            return EMPTY_BYTES;
        }
        byte[] bs = null;
        try {
            bs = s.getBytes(charset);
        } catch (Exception e) {
            // ignore
        }
        return bs;
    }

    public static final String bytes2String(byte[] bs, String charset) {
        if (null == bs) {
            return EMPTY_STRING;
        }
        String s = null;
        try {
            s = new String(bs, charset);
        } catch (Exception e) {
            // ignore
        }
        return s;
    }
}
