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
 *//*


*/
/**
 * $Id: EndTransactionResponseHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 *//*

package org.apache.rocketmq.common.protocol.header.mqtt;

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class MqttHeader implements CommandCustomHeader {

    //fix header members
    @CFNotNull
    private MqttMessageType messageType;
    @CFNotNull
    private boolean isDup;
    @CFNotNull
    private MqttQoS qosLevel;
    @CFNotNull
    private boolean isRetain;
    @CFNotNull
    private int remainingLength;

    //variable header members
    private MqttConnectReturnCode connectReturnCode;
    private boolean sessionPresent;
    private String name;
    private Integer version;
    private boolean hasUserName;
    private boolean hasPassword;
    private boolean isWillRetain;
    private Integer willQos;
    private boolean isWillFlag;
    private boolean isCleanSession;
    private Integer keepAliveTimeSeconds;
    private Integer messageId;
    private String topicName;
    private Integer packetId;

    public MqttMessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MqttMessageType messageType) {
        this.messageType = messageType;
    }

    public boolean isDup() {
        return isDup;
    }

    public void setDup(boolean dup) {
        isDup = dup;
    }

    public MqttQoS getQosLevel() {
        return qosLevel;
    }

    public void setQosLevel(MqttQoS qosLevel) {
        this.qosLevel = qosLevel;
    }

    public boolean isRetain() {
        return isRetain;
    }

    public void setRetain(boolean retain) {
        isRetain = retain;
    }

    public int getRemainingLength() {
        return remainingLength;
    }

    public void setRemainingLength(int remainingLength) {
        this.remainingLength = remainingLength;
    }

    public MqttConnectReturnCode getConnectReturnCode() {
        return connectReturnCode;
    }

    public void setConnectReturnCode(MqttConnectReturnCode connectReturnCode) {
        this.connectReturnCode = connectReturnCode;
    }

    public boolean isSessionPresent() {
        return sessionPresent;
    }

    public void setSessionPresent(boolean sessionPresent) {
        this.sessionPresent = sessionPresent;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public boolean isHasUserName() {
        return hasUserName;
    }

    public void setHasUserName(boolean hasUserName) {
        this.hasUserName = hasUserName;
    }

    public boolean isHasPassword() {
        return hasPassword;
    }

    public void setHasPassword(boolean hasPassword) {
        this.hasPassword = hasPassword;
    }

    public boolean isWillRetain() {
        return isWillRetain;
    }

    public void setWillRetain(boolean willRetain) {
        isWillRetain = willRetain;
    }

    public Integer getWillQos() {
        return willQos;
    }

    public void setWillQos(Integer willQos) {
        this.willQos = willQos;
    }

    public boolean isWillFlag() {
        return isWillFlag;
    }

    public void setWillFlag(boolean willFlag) {
        isWillFlag = willFlag;
    }

    public boolean isCleanSession() {
        return isCleanSession;
    }

    public void setCleanSession(boolean cleanSession) {
        isCleanSession = cleanSession;
    }

    public Integer getKeepAliveTimeSeconds() {
        return keepAliveTimeSeconds;
    }

    public void setKeepAliveTimeSeconds(Integer keepAliveTimeSeconds) {
        this.keepAliveTimeSeconds = keepAliveTimeSeconds;
    }

    public Integer getMessageId() {
        return messageId;
    }

    public void setMessageId(Integer messageId) {
        this.messageId = messageId;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public Integer getPacketId() {
        return packetId;
    }

    public void setPacketId(Integer packetId) {
        this.packetId = packetId;
    }

    @Override
    public void checkFields() throws RemotingCommandException {

    }

}
*/
