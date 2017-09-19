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

package org.apache.rocketmq.remoting.api.channel;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

public interface ChunkRegion {
    void release();

    /**
     * @return Returns the offset in the file where the transfer began.
     */
    long position();

    /**
     * @return Return the bytes which was transferred already
     */
    long transferred();

    /**
     * @return Returns the number of bytes to transfer.
     */
    long count();

    /**
     * Transfers the content of this file region to the specified channel.
     *
     * @param target the destination of the transfer
     * @param position the relative offset of the file where the transfer begins
     * from. For example, <tt>0</tt> will make the transfer start
     * from {@link #position()}th byte and
     * <tt>{@link #count()} - 1</tt> will make the last byte of the
     * region transferred.
     * @return the length of the transferred file region
     * @throws IOException IOException
     */
    long transferTo(WritableByteChannel target, long position) throws IOException;
}
