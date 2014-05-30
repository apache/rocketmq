package com.alibaba.common.lang.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;

/**
 * 高效的<code>PipedInputStream</code>替换方案. 这个类提供了较标准实现更为宽松的约定, 换取了大大提高的性能. 支持非阻塞的操作. 本代码移植自IBM
 * developer works精彩文章, 参见package文档.
 *
 * @author Michael Zhou
 * @version $Id: PipedInputStream.java 509 2004-02-16 05:42:07Z baobao $
 */
public class PipedInputStream extends InputStream {
    // default values
    private static final int   DEFAULT_BUFFER_SIZE = 8192;
    private static final float DEFAULT_HYSTERESIS = 0.75f;
    private static final int   DEFAULT_TIMEOUT_MS = 1000;

    // flag indicates whether method applies to reader or writer
    private static final boolean READER = false;

    // flag indicates whether method applies to reader or writer
    private static final boolean WRITER = true;

    // internal pipe buffer
    private byte[] buffer;

    // read/write index
    private int readx;

    // read/write index
    private int writex;

    // pipe capacity, hysteresis level
    private int capacity;

    // pipe capacity, hysteresis level
    private int level;

    // flags
    private boolean eof;

    // flags
    private boolean closed;

    // flags
    private boolean sleeping;

    // flags
    private boolean nonBlocking;

    // reader/writer thread
    private Thread reader;

    // reader/writer thread
    private Thread writer;

    // pending exception
    private IOException exception;

    // deadlock-breaking timeout
    private int timeout = DEFAULT_TIMEOUT_MS;

    public PipedInputStream() {
        this(DEFAULT_BUFFER_SIZE, DEFAULT_HYSTERESIS);
    }

    public PipedInputStream(int bufferSize) {
        this(bufferSize, DEFAULT_HYSTERESIS);
    }

    // e.g., hysteresis .75 means sleeping reader/writer is not
    // immediately woken until the buffer is 75% full/empty
    public PipedInputStream(int bufferSize, float hysteresis) {
        if ((hysteresis < 0.0) || (hysteresis > 1.0)) {
            throw new IllegalArgumentException("Hysteresis: " + hysteresis);
        }

        capacity     = bufferSize;
        buffer       = new byte[capacity];
        level        = (int) (bufferSize * hysteresis);
    }

    public void setTimeout(int ms) {
        this.timeout = ms;
    }

    public void setNonBlocking(boolean nonBlocking) {
        this.nonBlocking = nonBlocking;
    }

    private byte[] one = new byte[1];

    public int read() throws IOException {
        // read 1 byte
        int amount = read(one, 0, 1);

        // return EOF / the byte
        return (amount < 0) ? (-1)
                            : (one[0] & 0xff);
    }

    public synchronized int read(byte[] data, int offset, int length)
            throws IOException {
        // take a reference to the reader thread
        if (reader == null) {
            reader = Thread.currentThread();
        }

        // check parameters
        if (data == null) {
            throw new NullPointerException();
        } else if ((offset < 0) || ((offset + length) > data.length) || (length < 0)) { // check indices
            throw new IndexOutOfBoundsException();
        } else {
            // throw an exception if the stream is closed
            closedCheck();

            // throw any pending exception
            exceptionCheck();

            if (length <= 0) {
                return 0;
            } else {
                // wait for some data to become available for reading
                int available = checkedAvailable(READER);

                // return -1 on EOF
                if (available < 0) {
                    return -1;
                }

                // calculate amount of contiguous data in pipe buffer
                int contiguous = capacity - (readx % capacity);

                // calculate how much we will read this time
                int amount = (length > available) ? available
                                                  : length;

                if (amount > contiguous) {
                    // two array copies needed if data wrap around the buffer
                    System.arraycopy(buffer, readx % capacity, data, offset, contiguous);
                    System.arraycopy(buffer, 0, data, offset + contiguous, amount - contiguous);
                } else {
                    // otherwise, one array copy needed
                    System.arraycopy(buffer, readx % capacity, data, offset, amount);
                }

                // update indices with amount of data read
                processed(READER, amount);

                // return amount read
                return amount;
            }
        }
    }

    public synchronized long skip(long amount) throws IOException {
        // take a reference to the reader thread
        if (reader == null) {
            reader = Thread.currentThread();
        }

        // throw an exception if the stream is closed
        closedCheck();

        // throw any pending exception
        exceptionCheck();

        if (amount <= 0) {
            return 0;
        } else {
            // wait for some data to become available for skipping
            int available = checkedAvailable(READER);

            // return 0 on EOF
            if (available < 0) {
                return 0;
            }

            // calculate how much we will skip this time
            if (amount > available) {
                amount = available;
            }

            // update indices with amount of data skipped
            processed(READER, (int) amount);

            // return amount skipped
            return amount;
        }
    }

    private void processed(boolean rw, int amount) {
        if (rw == READER) {
            // update read index with amount read
            readx = (readx + amount) % (capacity * 2);
        } else {
            // update write index with amount read
            writex = (writex + amount) % (capacity * 2);
        }

        // check whether a thread is sleeping and we have reached the
        // hysteresis threshold
        if (sleeping && (available(!rw) >= level)) {
            // wake sleeping thread
            notify();
            sleeping = false;
        }
    }

    public synchronized int available() throws IOException {
        // throw an exception if the stream is closed
        closedCheck();

        // throw any pending exception
        exceptionCheck();

        // determine how much can be read
        int amount = available(READER);

        // return 0 on EOF, otherwise the amount readable
        return (amount < 0) ? 0
                            : amount;
    }

    private int checkedAvailable(boolean rw) throws IOException {
        // always called from synchronized(this) method
        try {
            int available;

            // loop while no data can be read/written
            while ((available = available(rw)) == 0) {
                if (rw == READER) { // reader

                    // throw any pending exception
                    exceptionCheck();
                } else { // writer

                    // throw an exception if the stream is closed
                    closedCheck();
                }

                // throw an exception if the pipe is broken
                brokenCheck(rw);

                if (!nonBlocking) { // blocking mode

                    // wake any sleeping thread
                    if (sleeping) {
                        notify();
                    }

                    // sleep for timeout ms (in case of peer thread death)
                    sleeping = true;
                    wait(timeout);

                    // timeout means that hysteresis may not be obeyed
                } else { // non-blocking mode

                    // throw an InterruptedIOException
                    throw new InterruptedIOException("Pipe " + (rw ? "full"
                                                                   : "empty"));
                }
            }

            return available;
        } catch (InterruptedException ex) {
            // rethrow InterruptedException as InterruptedIOException
            throw new InterruptedIOException(ex.getMessage());
        }
    }

    private int available(boolean rw) {
        // calculate amount of space used in pipe
        int used = ((writex + (capacity * 2)) - readx) % (capacity * 2);

        if (rw == WRITER) { // writer

            // return amount of space available for writing
            return capacity - used;
        } else { // reader

            // return amount of data in pipe or -1 at EOF
            return (eof && (used == 0)) ? (-1)
                                        : used;
        }
    }

    public void close() throws IOException {
        // close the read end of this pipe
        close(READER);
    }

    private synchronized void close(boolean rw) throws IOException {
        if (rw == READER) { // reader

            // set closed flag
            closed = true;
        } else if (!eof) { // writer

            // set eof flag
            eof = true;

            // check if data remain unread
            if (available(READER) > 0) {
                // throw an exception if the reader has already closed the pipe
                closedCheck();

                // throw an exception if the reader thread has died
                brokenCheck(WRITER);
            }
        }

        // wake any sleeping thread
        if (sleeping) {
            notify();
            sleeping = false;
        }
    }

    private void exceptionCheck() throws IOException {
        // throw any pending exception
        if (exception != null) {
            IOException ex = exception;

            exception = null;
            throw ex; // could wrap ex in a local exception
        }
    }

    private void closedCheck() throws IOException {
        // throw an exception if the pipe is closed
        if (closed) {
            throw new IOException("Stream closed");
        }
    }

    private void brokenCheck(boolean rw) throws IOException {
        // get a reference to the peer thread
        Thread thread = (rw == WRITER) ? reader
                                       : writer;

        // throw an exception if  the peer thread has died
        if ((thread != null) && !thread.isAlive()) {
            throw new IOException("Broken pipe");
        }
    }

    private synchronized void writeImpl(byte[] data, int offset, int length)
            throws IOException {
        // take a reference to the writer thread
        if (writer == null) {
            writer = Thread.currentThread();
        }

        // throw an exception if the stream is closed
        if (eof || closed) {
            throw new IOException("Stream closed");
        } else {
            int written = 0;

            try {
                // loop to write all the data
                do {
                    // wait for space to become available for writing
                    int available = checkedAvailable(WRITER);

                    // calculate amount of contiguous space in pipe buffer
                    int contiguous = capacity - (writex % capacity);

                    // calculate how much we will write this time
                    int amount = (length > available) ? available
                                                      : length;

                    if (amount > contiguous) {
                        // two array copies needed if space wraps around the buffer
                        System.arraycopy(data, offset, buffer, writex % capacity, contiguous);
                        System.arraycopy(data, offset + contiguous, buffer, 0, amount - contiguous);
                    } else {
                        // otherwise, one array copy needed
                        System.arraycopy(data, offset, buffer, writex % capacity, amount);
                    }

                    // update indices with amount of data read
                    processed(WRITER, amount);

                    // update amount written by this method
                    written += amount;
                } while (written < length);

                // data successfully written
            } catch (InterruptedIOException ex) {
                // write operation was interrupted; set the bytesTransferred
                // exception field to reflect the amount of data written
                ex.bytesTransferred = written;

                // rethrow exception
                throw ex;
            }
        }
    }

    private synchronized void setException(IOException ex) throws IOException {
        // fail if an exception is already pending
        if (exception != null) {
            throw new IOException("Exception already set: " + exception);
        }

        // throw an exception if the pipe is broken
        brokenCheck(WRITER);

        // take a reference to the pending exception
        this.exception = ex;

        // wake any sleeping thread
        if (sleeping) {
            notify();
            sleeping = false;
        }
    }

    public OutputStreamEx getOutputStream() {
        // return an OutputStreamImpl associated with this pipe
        return new OutputStreamImpl();
    }

    private class OutputStreamImpl extends OutputStreamEx {
        private byte[] one = new byte[1];

        public void write(int datum) throws IOException {
            // write one byte using internal array
            one[0] = (byte) datum;
            write(one, 0, 1);
        }

        public void write(byte[] data, int offset, int length)
                throws IOException {
            // check parameters
            if (data == null) {
                throw new NullPointerException();
            } else if ((offset < 0) || ((offset + length) > data.length) || (length < 0)) {
                throw new IndexOutOfBoundsException();
            } else if (length > 0) {
                // call through to writeImpl()
                PipedInputStream.this.writeImpl(data, offset, length);
            }
        }

        public void close() throws IOException {
            // close the write end of this pipe
            PipedInputStream.this.close(WRITER);
        }

        public void setException(IOException ex) throws IOException {
            // set a pending exception
            PipedInputStream.this.setException(ex);
        }
    }

    // static OutputStream extension with setException() method
    public abstract static class OutputStreamEx extends OutputStream {
        public abstract void setException(IOException ex) throws IOException;
    }
}
