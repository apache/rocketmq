package org.apache.rocketmq.common.message;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;

public class MessageExt extends Message {
    private static final long serialVersionUID = 5720810158625748049L;

    private String brokerName;
    private int queueId;
    private int storeSize;
    private long queueOffset;
    private int sysFlag;
    private long bornTimestamp;
    private SocketAddress bornHost;
    private long storeTimestamp;
    private SocketAddress storeHost;
    private String msgId;
    private long commitLogOffset;
    private int bodyCRC;
    private int reconsumeTimes;
    private long preparedTransactionOffset;

    public MessageExt() {
    }

    public MessageExt(int queueId, long bornTimestamp, SocketAddress bornHost, long storeTimestamp,
                      SocketAddress storeHost, String msgId) {
        this.queueId = queueId;
        this.bornTimestamp = bornTimestamp;
        this.bornHost = bornHost;
        this.storeTimestamp = storeTimestamp;
        this.storeHost = storeHost;
        this.msgId = msgId;
    }

    public static TopicFilterType parseTopicFilterType(final int sysFlag) {
        return (sysFlag & MessageSysFlag.MULTI_TAGS_FLAG) == MessageSysFlag.MULTI_TAGS_FLAG ?
                TopicFilterType.MULTI_TAG : TopicFilterType.SINGLE_TAG;
    }

    public static ByteBuffer socketAddress2ByteBuffer(final SocketAddress socketAddress, final ByteBuffer byteBuffer) {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        InetAddress address = inetSocketAddress.getAddress();
        byteBuffer.put(address.getAddress());
        byteBuffer.putInt(inetSocketAddress.getPort());
        byteBuffer.flip();
        return byteBuffer;
    }

    public static ByteBuffer socketAddress2ByteBuffer(SocketAddress socketAddress) {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        InetAddress address = inetSocketAddress.getAddress();
        ByteBuffer byteBuffer = ByteBuffer.allocate(address.getAddress().length + 4);
        return socketAddress2ByteBuffer(socketAddress, byteBuffer);
    }

    public ByteBuffer getBornHostBytes() {
        return socketAddress2ByteBuffer(this.bornHost);
    }

    public ByteBuffer getBornHostBytes(ByteBuffer byteBuffer) {
        return socketAddress2ByteBuffer(this.bornHost, byteBuffer);
    }

    public ByteBuffer getStoreHostBytes() {
        return socketAddress2ByteBuffer(this.storeHost);
    }

    public ByteBuffer getStoreHostBytes(ByteBuffer byteBuffer) {
        return socketAddress2ByteBuffer(this.storeHost, byteBuffer);
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public SocketAddress getBornHost() {
        return bornHost;
    }

    public void setBornHost(SocketAddress bornHost) {
        this.bornHost = bornHost;
    }

    public String getBornHostString() {
        if (this.bornHost != null) {
            InetAddress inetAddress = ((InetSocketAddress) this.bornHost).getAddress();
            return inetAddress != null ? inetAddress.getHostAddress() : null;
        }
        return null;
    }

    public String getBornHostNameString() {
        if (this.bornHost != null) {
            if (this.bornHost instanceof InetSocketAddress) {
                return ((InetSocketAddress) this.bornHost).getHostString();
            }
            InetAddress inetAddress = ((InetSocketAddress) this.bornHost).getAddress();
            return inetAddress != null ? inetAddress.getHostName() : null;
        }
        return null;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public SocketAddress getStoreHost() {
        return storeHost;
    }

    public void setStoreHost(SocketAddress storeHost) {
        this.storeHost = storeHost;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public int getSysFlag() {
        return sysFlag;
    }

    public void setSysFlag(int sysFlag) {
        this.sysFlag = sysFlag;
    }

    public void setStoreHostAddressV6Flag() {
        this.sysFlag |= MessageSysFlag.STOREHOSTADDRESS_V6_FLAG;
    }

    public void setBornHostV6Flag() {
        this.sysFlag |= MessageSysFlag.BORNHOST_V6_FLAG;
    }

    public int getBodyCRC() {
        return bodyCRC;
    }

    public void setBodyCRC(int bodyCRC) {
        this.bodyCRC = bodyCRC;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public void setCommitLogOffset(long commitLogOffset) {
        this.commitLogOffset = commitLogOffset;
    }

    public int getStoreSize() {
        return storeSize;
    }

    public void setStoreSize(int storeSize) {
        this.storeSize = storeSize;
    }

    public int getReconsumeTimes() {
        return reconsumeTimes;
    }

    public void setReconsumeTimes(int reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }

    public long getPreparedTransactionOffset() {
        return preparedTransactionOffset;
    }

    public void setPreparedTransactionOffset(long preparedTransactionOffset) {
        this.preparedTransactionOffset = preparedTransactionOffset;
    }

    public Integer getTopicSysFlag() {
        String topicSysFlagString = getProperty(MessageConst.PROPERTY_TRANSIENT_TOPIC_CONFIG);
        if (topicSysFlagString != null && !topicSysFlagString.isEmpty()) {
            return Integer.valueOf(topicSysFlagString);
        }
        return null;
    }

    public void setTopicSysFlag(Integer topicSysFlag) {
        if (topicSysFlag == null) {
            clearProperty(MessageConst.PROPERTY_TRANSIENT_TOPIC_CONFIG);
        } else {
            putProperty(MessageConst.PROPERTY_TRANSIENT_TOPIC_CONFIG, String.valueOf(topicSysFlag));
        }
    }

    public Integer getGroupSysFlag() {
        String groupSysFlagString = getProperty(MessageConst.PROPERTY_TRANSIENT_GROUP_CONFIG);
        if (groupSysFlagString != null && !groupSysFlagString.isEmpty()) {
            return Integer.valueOf(groupSysFlagString);
        }
        return null;
    }

    public void setGroupSysFlag(Integer groupSysFlag) {
        if (groupSysFlag == null) {
            clearProperty(MessageConst.PROPERTY_TRANSIENT_GROUP_CONFIG);
        } else {
            putProperty(MessageConst.PROPERTY_TRANSIENT_GROUP_CONFIG, String.valueOf(groupSysFlag));
        }
    }

    @Override
    public String toString() {
        return "MessageExt [brokerName=" + brokerName + ", queueId=" + queueId + ", storeSize=" + storeSize +
                ", queueOffset=" + queueOffset + ", sysFlag=" + sysFlag + ", bornTimestamp=" + bornTimestamp +
                ", bornHost=" + bornHost + ", storeTimestamp=" + storeTimestamp + ", storeHost=" + storeHost +
                ", msgId=" + msgId + ", commitLogOffset=" + commitLogOffset + ", bodyCRC=" + bodyCRC +
                ", reconsumeTimes=" + reconsumeTimes + ", preparedTransactionOffset=" + preparedTransactionOffset +
                ", toString()=" + super.toString() + "]";
    }
}
