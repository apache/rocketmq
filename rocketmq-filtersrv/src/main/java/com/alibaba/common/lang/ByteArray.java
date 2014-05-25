package com.alibaba.common.lang;

import com.alibaba.common.lang.io.ByteArrayInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 代表一个byte数组。
 *
 * @author Michael Zhou
 * @version $Id: ByteArray.java 593 2004-02-26 13:47:19Z baobao $
 */
public class ByteArray {
    private byte[] bytes;
    private int    offset;
    private int    length;

    public ByteArray(byte[] bytes, int offset, int length) {
        this.bytes      = bytes;
        this.offset     = offset;
        this.length     = length;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public int getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }

    public byte[] toByteArray() {
        byte[] copy = new byte[length];

        System.arraycopy(bytes, offset, copy, 0, length);

        return copy;
    }

    public InputStream toInputStream() {
        return new ByteArrayInputStream(bytes, offset, length);
    }

    public void writeTo(OutputStream out) throws IOException {
        out.write(bytes, offset, length);
    }
}
