package com.alibaba.common.lang.io;

import com.alibaba.common.lang.ByteArray;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 非同步的<code>ByteArrayOutputStream</code>替换方案, 执行<code>toByteArray()</code>方法时返回的是只读的内部字节数组,
 * 避免了没有必要的字节复制. 本代码移植自IBM developer works精彩文章, 参见package文档.
 *
 * @author Michael Zhou
 * @version $Id: ByteArrayOutputStream.java 593 2004-02-26 13:47:19Z baobao $
 */
public class ByteArrayOutputStream extends OutputStream {
    private static final int DEFAULT_INITIAL_BUFFER_SIZE = 8192;

    // internal buffer
    private byte[] buffer;
    private int    index;
    private int    capacity;

    // is the stream closed?
    private boolean closed;

    // is the buffer shared?
    private boolean shared;

    public ByteArrayOutputStream() {
        this(DEFAULT_INITIAL_BUFFER_SIZE);
    }

    public ByteArrayOutputStream(int initialBufferSize) {
        capacity     = initialBufferSize;
        buffer       = new byte[capacity];
    }

    public void write(int datum) throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        } else {
            if (index >= capacity) {
                // expand the internal buffer
                capacity = (capacity * 2) + 1;

                byte[] tmp = new byte[capacity];

                System.arraycopy(buffer, 0, tmp, 0, index);
                buffer     = tmp;

                // the new buffer is not shared
                shared = false;
            }

            // store the byte
            buffer[index++] = (byte) datum;
        }
    }

    public void write(byte[] data, int offset, int length) throws IOException {
        if (data == null) {
            throw new NullPointerException();
        } else if ((offset < 0) || ((offset + length) > data.length) || (length < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (closed) {
            throw new IOException("Stream closed");
        } else {
            if ((index + length) > capacity) {
                // expand the internal buffer
                capacity = (capacity * 2) + length;

                byte[] tmp = new byte[capacity];

                System.arraycopy(buffer, 0, tmp, 0, index);
                buffer     = tmp;

                // the new buffer is not shared
                shared = false;
            }

            // copy in the subarray
            System.arraycopy(data, offset, buffer, index, length);
            index += length;
        }
    }

    public void close() {
        closed = true;
    }

    public void writeTo(OutputStream out) throws IOException {
        // write the internal buffer directly
        out.write(buffer, 0, index);
    }

    public ByteArray toByteArray() {
        shared = true;
        return new ByteArray(buffer, 0, index);
    }

    public InputStream toInputStream() {
        // return a stream reading from the shared internal buffer
        shared = true;
        return new ByteArrayInputStream(buffer, 0, index);
    }

    public void reset() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        } else {
            if (shared) {
                // create a new buffer if it is shared
                buffer     = new byte[capacity];
                shared     = false;
            }

            // reset index
            index = 0;
        }
    }
}
