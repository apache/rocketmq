package org.apache.rocketmq.common.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Entry Checkpoint file util
 * Format:
 *   First line: Entries size
 *   Then: every Entry(String)
 */
public class CheckpointFile<T> {

    public interface CheckpointSerializer<T> {
        /**
         * Serialize entry to line
         */
        String toLine(final T entry);

        /**
         * DeSerialize line to entry
         */
        T fromLine(final String line);
    }

    private final String path;
    private final CheckpointSerializer<T> serializer;

    public CheckpointFile(final String path, final CheckpointSerializer<T> serializer) {
        this.path = path;
        this.serializer = serializer;
    }

    /**
     * Write entries to file
     */
    public void write(final List<T> entries) throws IOException  {
        if (entries.isEmpty()) {
            return;
        }
        synchronized (this) {
            final FileOutputStream fos = new FileOutputStream(this.path);
            final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8));
            try {
                // Write size
                writer.write(entries.size() + "");
                writer.newLine();

                // Write entries
                for (T entry : entries) {
                    final String line = this.serializer.toLine(entry);
                    if (line != null && !line.isEmpty()) {
                        writer.write(line);
                        writer.newLine();
                    }
                }

                writer.flush();
                fos.getFD().sync();
            } finally {
                writer.close();
            }
        }
    }

    /**
     * Read entries from file
     */
    public List<T> read() throws IOException {
        final ArrayList<T> result = new ArrayList<>();
        synchronized(this) {
            final File file = new File(this.path);
            if (!file.exists()) {
                return result;
            }
            final BufferedReader reader = Files.newBufferedReader(file.toPath());
            try {
                // Read size
                int expectedLines = Integer.parseInt(reader.readLine());

                // Read entries
                String line = reader.readLine();
                while (line != null) {
                    final T entry = this.serializer.fromLine(line);
                    if (entry != null) {
                        result.add(entry);
                    }
                    line = reader.readLine();
                }
                if (result.size() != expectedLines) {
                    final String err = String.format("Expect %d entries, only found %d entries", expectedLines, result.size());
                    throw new IOException(err);
                }
                return result;
            } finally {
                reader.close();
            }
        }
    }
}
