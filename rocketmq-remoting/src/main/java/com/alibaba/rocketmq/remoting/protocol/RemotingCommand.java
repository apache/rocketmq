/**
 * $Id: RemotingCommand.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.remoting.protocol;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.CommandHeader;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.FlagBit;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.LanguageCode;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.NVPair;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode;
import com.google.protobuf.InvalidProtocolBufferException;


/**
 * RPC 请求应答命令
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class RemotingCommand {
    public static String RemotingVersionKey = "rocketmq.remoting.version";
    private static volatile int ConfigVersion = -1;
    private static AtomicInteger RequestId = new AtomicInteger(0);

    /**
     * Header 部分
     */
    private int code;
    private LanguageCode language = LanguageCode.JAVA;
    private int version = 0;
    private int opaque = RequestId.getAndIncrement();
    private int flag = 0;
    private String remark;
    private List<NVPair> extFields;
    private CommandCustomHeader customHeader;

    /**
     * Body 部分
     */
    private byte[] body;


    protected RemotingCommand() {
    }


    public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.customHeader = customHeader;
        setCmdVersion(cmd);
        return cmd;
    }


    public static RemotingCommand createResponseCommand(Class<? extends CommandCustomHeader> classHeader) {
        RemotingCommand cmd =
                createResponseCommand(ResponseCode.SYSTEM_ERROR_VALUE, "not set any response code", classHeader);

        return cmd;
    }


    public static RemotingCommand createResponseCommand(int code, String remark) {
        return createResponseCommand(code, remark, null);
    }


    /**
     * 只有通信层内部会调用，业务不会调用
     */
    public static RemotingCommand createResponseCommand(int code, String remark,
            Class<? extends CommandCustomHeader> classHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.markResponseType();
        cmd.setCode(code);
        cmd.setRemark(remark);
        setCmdVersion(cmd);

        if (classHeader != null) {
            try {
                CommandCustomHeader objectHeader = classHeader.newInstance();
                cmd.customHeader = objectHeader;
            }
            catch (InstantiationException e) {
                return null;
            }
            catch (IllegalAccessException e) {
                return null;
            }
        }

        return cmd;
    }


    private static void setCmdVersion(RemotingCommand cmd) {
        if (ConfigVersion >= 0) {
            cmd.setVersion(ConfigVersion);
        }
        else {
            String v = System.getProperty(RemotingVersionKey);
            if (v != null) {
                int value = Integer.parseInt(v);
                cmd.setVersion(value);
                ConfigVersion = value;
            }
        }
    }


    private void makeCustomHeaderToNet() {
        if (this.customHeader != null) {
            Field[] fields = this.customHeader.getClass().getDeclaredFields();
            this.extFields = new ArrayList<NVPair>(fields.length);
            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String name = field.getName();
                    if (!name.startsWith("this")) {
                        Object value = null;
                        try {
                            field.setAccessible(true);
                            value = field.get(this.customHeader);
                        }
                        catch (IllegalArgumentException e) {
                        }
                        catch (IllegalAccessException e) {
                        }

                        if (value != null) {
                            NVPair.Builder nvb = NVPair.newBuilder();
                            nvb.setName(name);
                            nvb.setValue(value.toString());
                            this.extFields.add(nvb.build());
                        }
                    }
                }
            }
        }
    }


    public CommandCustomHeader getCustomHeader() {
        return customHeader;
    }


    public CommandCustomHeader decodeCommandCustomHeader(Class<? extends CommandCustomHeader> classHeader)
            throws RemotingCommandException {
        if (this.extFields != null) {
            CommandCustomHeader objectHeader;
            try {
                objectHeader = classHeader.newInstance();
            }
            catch (InstantiationException e) {
                return null;
            }
            catch (IllegalAccessException e) {
                return null;
            }

            for (NVPair nvp : this.extFields) {
                String name = nvp.getName();
                String value = nvp.getValue();

                try {
                    Field field = objectHeader.getClass().getDeclaredField(name);
                    field.setAccessible(true);
                    String type = field.getType().getSimpleName();
                    Object valueParsed = null;

                    if (type.equals("String")) {
                        valueParsed = value;
                    }
                    else if (type.equals("Integer")) {
                        valueParsed = Integer.parseInt(value);
                    }
                    else if (type.equals("Long")) {
                        valueParsed = Long.parseLong(value);
                    }
                    else if (type.equals("Boolean")) {
                        valueParsed = Boolean.parseBoolean(value);
                    }
                    else if (type.equals("Double")) {
                        valueParsed = Double.parseDouble(value);
                    }
                    else if (type.equals("int")) {
                        valueParsed = Integer.parseInt(value);
                    }
                    else if (type.equals("long")) {
                        valueParsed = Long.parseLong(value);
                    }
                    else if (type.equals("boolean")) {
                        valueParsed = Boolean.parseBoolean(value);
                    }
                    else if (type.equals("double")) {
                        valueParsed = Double.parseDouble(value);
                    }

                    field.set(objectHeader, valueParsed);
                }
                catch (SecurityException e) {
                }
                catch (NoSuchFieldException e) {
                }
                catch (IllegalArgumentException e) {
                }
                catch (IllegalAccessException e) {
                }
            }

            // 检查返回对象是否有效
            Field[] fields = objectHeader.getClass().getDeclaredFields();
            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String name = field.getName();
                    if (!name.startsWith("this")) {
                        Object value = null;
                        try {
                            field.setAccessible(true);
                            value = field.get(objectHeader);
                        }
                        catch (IllegalArgumentException e) {
                        }
                        catch (IllegalAccessException e) {
                        }

                        // 空值检查
                        if (null == value) {
                            Annotation[] ann = field.getAnnotations();
                            if (ann != null && ann.length > 0) {
                                if (ann[0].annotationType().getSimpleName()
                                    .equalsIgnoreCase(CFNotNull.class.getSimpleName())) {
                                    throw new RemotingCommandException(name + " is null");
                                }
                            }
                        }
                    }
                }
            }

            objectHeader.checkFields();

            return objectHeader;
        }

        return null;
    }


    private CommandHeader buildHeader() {
        CommandHeader.Builder builder = CommandHeader.newBuilder();
        builder.setCode(this.code);
        builder.setLanguage(this.language);
        builder.setVersion(this.version);
        builder.setOpaque(this.opaque);
        builder.setFlag(this.flag);
        if (this.remark != null) {
            builder.setRemark(this.remark);
        }

        // customHeader
        this.makeCustomHeaderToNet();

        // extFields
        if (this.extFields != null) {
            int i = 0;
            for (NVPair nv : this.extFields) {
                builder.addExtFields(i++, nv);
            }
        }

        return builder.build();
    }


    public ByteBuffer encode() {
        CommandHeader header = this.buildHeader();

        // 1> header length size
        int length = 4;

        // 2> header data length
        byte[] headerData = header.toByteArray();
        length += headerData.length;

        // 3> body data length
        if (this.body != null) {
            length += body.length;
        }

        ByteBuffer result = ByteBuffer.allocate(4 + length);

        // length
        result.putInt(length);

        // header length
        result.putInt(headerData.length);

        // header data
        result.put(headerData);

        // body data;
        if (this.body != null) {
            result.put(this.body);
        }

        result.flip();

        return result;
    }


    public ByteBuffer encodeHeader() {
        return encodeHeader(this.body != null ? this.body.length : 0);
    }


    /**
     * 只打包Header，body部分独立传输
     */
    public ByteBuffer encodeHeader(final int bodyLength) {
        CommandHeader header = this.buildHeader();

        // 1> header length size
        int length = 4;

        // 2> header data length
        byte[] headerData = header.toByteArray();
        length += headerData.length;

        // 3> body data length
        length += bodyLength;

        ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

        // length
        result.putInt(length);

        // header length
        result.putInt(headerData.length);

        // header data
        result.put(headerData);

        result.flip();

        return result;
    }


    public static RemotingCommand decode(final byte[] array) throws InvalidProtocolBufferException {
        ByteBuffer byteBuffer = ByteBuffer.wrap(array);
        return decode(byteBuffer);
    }


    public static RemotingCommand decode(final ByteBuffer byteBuffer) throws InvalidProtocolBufferException {
        int length = byteBuffer.limit();
        int headerLength = byteBuffer.getInt();

        byte[] headerData = new byte[headerLength];
        byteBuffer.get(headerData);

        int bodyLength = length - 4 - headerLength;
        byte[] bodyData = null;
        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            byteBuffer.get(bodyData);
        }

        RemotingCommand cmd = new RemotingCommand();
        CommandHeader header = CommandHeader.parseFrom(headerData);

        cmd.code = header.getCode();
        cmd.language = header.getLanguage();
        cmd.version = header.getVersion();
        cmd.opaque = header.getOpaque();
        cmd.flag = header.getFlag();
        if (header.hasRemark())
            cmd.remark = header.getRemark();

        cmd.extFields = header.getExtFieldsList();

        cmd.body = bodyData;

        return cmd;
    }


    public void markResponseType() {
        int bits = 1 << FlagBit.RPC_TYPE_VALUE;
        this.flag |= bits;
    }


    public boolean isResponseType() {
        int bits = 1 << FlagBit.RPC_TYPE_VALUE;
        return (this.flag & bits) == bits;
    }


    public void markOnewayRPC() {
        int bits = 1 << FlagBit.RPC_ONEWAY_VALUE;
        this.flag |= bits;
    }


    public boolean isOnewayRPC() {
        int bits = 1 << FlagBit.RPC_ONEWAY_VALUE;
        return (this.flag & bits) == bits;
    }


    public int getCode() {
        return code;
    }


    public void setCode(int code) {
        this.code = code;
    }


    public RemotingCommandType getType() {
        if (this.isResponseType()) {
            return RemotingCommandType.RESPONSE_COMMAND;
        }

        return RemotingCommandType.REQUEST_COMMAND;
    }


    public LanguageCode getLanguage() {
        return language;
    }


    public void setLanguage(LanguageCode language) {
        this.language = language;
    }


    public int getVersion() {
        return version;
    }


    public void setVersion(int version) {
        this.version = version;
    }


    public int getOpaque() {
        return opaque;
    }


    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }


    public int getFlag() {
        return flag;
    }


    public void setFlag(int flag) {
        this.flag = flag;
    }


    public String getRemark() {
        return remark;
    }


    public void setRemark(String remark) {
        this.remark = remark;
    }


    public byte[] getBody() {
        return body;
    }


    public void setBody(byte[] body) {
        this.body = body;
    }


    public List<NVPair> getExtFields() {
        return extFields;
    }


    public void setExtFields(List<NVPair> extFields) {
        this.extFields = extFields;
    }


    @Override
    public String toString() {
        return "RemotingCommand [code=" + code + ", language=" + language + ", version=" + version + ", opaque="
                + opaque + ", flag(B)=" + Integer.toBinaryString(flag) + ", remark=" + remark + ", extFields="
                + extFields + "]";
    }

    // public CommandCustomHeader getCustomHeader() {
    // return customHeader;
    // }
    //
    //
    // public void setCustomHeader(CommandCustomHeader customHeader) {
    // this.customHeader = customHeader;
    // }

}
