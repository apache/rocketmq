package com.alibaba.common.lang.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

/**
 * 将数据从任意<code>Reader</code>复制到<code>OutputStreamWriter</code>的输出引擎. 本代码移植自IBM developer works精彩文章,
 * 参见package文档.
 *
 * @author Michael Zhou
 * @version $Id: ReaderOutputEngine.java 509 2004-02-16 05:42:07Z baobao $
 */
public class ReaderOutputEngine implements OutputEngine {
    private static final int    DEFAULT_BUFFER_SIZE = 8192;
    private Reader              reader;
    private String              encoding;
    private OutputStreamFactory factory;
    private char[]              buffer;
    private Writer              writer;

    public ReaderOutputEngine(Reader reader) {
        this(reader, null, null, DEFAULT_BUFFER_SIZE);
    }

    public ReaderOutputEngine(Reader reader, OutputStreamFactory factory) {
        this(reader, factory, null, DEFAULT_BUFFER_SIZE);
    }

    public ReaderOutputEngine(Reader reader, OutputStreamFactory factory, String encoding) {
        this(reader, factory, encoding, DEFAULT_BUFFER_SIZE);
    }

    public ReaderOutputEngine(Reader reader, OutputStreamFactory factory, String encoding,
        int bufferSize) {
        this.reader       = reader;
        this.encoding     = encoding;
        this.factory      = (factory == null) ? DEFAULT_OUTPUT_STREAM_FACTORY
                                              : factory;
        buffer = new char[bufferSize];
    }

    public void open(OutputStream out) throws IOException {
        if (writer != null) {
            throw new IOException("Already initialized");
        } else {
            writer = (encoding == null) ? new OutputStreamWriter(factory.getOutputStream(out))
                                        : new OutputStreamWriter(factory.getOutputStream(out),
                    encoding);
        }
    }

    public void execute() throws IOException {
        if (writer == null) {
            throw new IOException("Not yet initialized");
        } else {
            int amount = reader.read(buffer);

            if (amount < 0) {
                writer.close();
            } else {
                writer.write(buffer, 0, amount);
            }
        }
    }

    public void close() throws IOException {
        reader.close();
    }
}
