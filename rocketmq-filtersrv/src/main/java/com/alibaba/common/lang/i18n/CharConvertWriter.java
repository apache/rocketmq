package com.alibaba.common.lang.i18n;

import java.io.FilterWriter;
import java.io.IOException;
import java.io.Writer;

public class CharConvertWriter extends FilterWriter {
    private CharConverter converter;

    public CharConvertWriter(Writer out, String converterName) {
        this(out, CharConverter.getInstance(converterName));
    }

    public CharConvertWriter(Writer out, CharConverter converter) {
        super(out);
        this.converter = converter;

        if (converter == null) {
            throw new NullPointerException("converter is null");
        }
    }

    public void write(char[] cbuf, int off, int len) throws IOException {
        char[] newbuf = new char[len];

        System.arraycopy(cbuf, off, newbuf, 0, len);

        converter.convert(newbuf, 0, len);

        super.write(newbuf, 0, len);
    }

    public void write(int c) throws IOException {
        super.write(converter.convert((char) c));
    }

    public void write(String str, int off, int len) throws IOException {
        super.write(converter.convert(str, off, len), 0, len);
    }
}
