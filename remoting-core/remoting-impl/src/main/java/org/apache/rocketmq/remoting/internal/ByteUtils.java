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

package org.apache.rocketmq.remoting.internal;

/**
 * Copy from Bouncy Castle Crypto APIs
 *
 * This class is a utility class for manipulating byte arrays.
 */
public final class ByteUtils {

    private static final char[] HEX_CHARS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    /**
     * Default constructor (private)
     */
    private ByteUtils() {
        // empty
    }

    /**
     * Compare two byte arrays (perform null checks beforehand).
     *
     * @param left the first byte array
     * @param right the second byte array
     * @return the result of the comparison
     */
    public static boolean equals(byte[] left, byte[] right) {
        if (left == null) {
            return right == null;
        }
        if (right == null) {
            return false;
        }

        if (left.length != right.length) {
            return false;
        }
        boolean result = true;
        for (int i = left.length - 1; i >= 0; i--) {
            result &= left[i] == right[i];
        }
        return result;
    }

    /**
     * Compare two two-dimensional byte arrays. No null checks are performed.
     *
     * @param left the first byte array
     * @param right the second byte array
     * @return the result of the comparison
     */
    public static boolean equals(byte[][] left, byte[][] right) {
        if (left.length != right.length) {
            return false;
        }

        boolean result = true;
        for (int i = left.length - 1; i >= 0; i--) {
            result &= ByteUtils.equals(left[i], right[i]);
        }

        return result;
    }

    /**
     * Compare two three-dimensional byte arrays. No null checks are performed.
     *
     * @param left the first byte array
     * @param right the second byte array
     * @return the result of the comparison
     */
    public static boolean equals(byte[][][] left, byte[][][] right) {
        if (left.length != right.length) {
            return false;
        }

        boolean result = true;
        for (int i = left.length - 1; i >= 0; i--) {
            if (left[i].length != right[i].length) {
                return false;
            }
            for (int j = left[i].length - 1; j >= 0; j--) {
                result &= ByteUtils.equals(left[i][j], right[i][j]);
            }
        }

        return result;
    }

    /**
     * Computes a hashcode based on the contents of a one-dimensional byte array
     * rather than its identity.
     *
     * @param array the array to compute the hashcode of
     * @return the hashcode
     */
    public static int deepHashCode(byte[] array) {
        int result = 1;
        for (int i = 0; i < array.length; i++) {
            result = 31 * result + array[i];
        }
        return result;
    }

    /**
     * Computes a hashcode based on the contents of a two-dimensional byte array
     * rather than its identity.
     *
     * @param array the array to compute the hashcode of
     * @return the hashcode
     */
    public static int deepHashCode(byte[][] array) {
        int result = 1;
        for (int i = 0; i < array.length; i++) {
            result = 31 * result + deepHashCode(array[i]);
        }
        return result;
    }

    /**
     * Computes a hashcode based on the contents of a three-dimensional byte
     * array rather than its identity.
     *
     * @param array the array to compute the hashcode of
     * @return the hashcode
     */
    public static int deepHashCode(byte[][][] array) {
        int result = 1;
        for (int i = 0; i < array.length; i++) {
            result = 31 * result + deepHashCode(array[i]);
        }
        return result;
    }

    /**
     * Return a clone of the given byte array (performs null check beforehand).
     *
     * @param array the array to clone
     * @return the clone of the given array, or <tt>null</tt> if the array is
     * <tt>null</tt>
     */
    public static byte[] clone(byte[] array) {
        if (array == null) {
            return null;
        }
        byte[] result = new byte[array.length];
        System.arraycopy(array, 0, result, 0, array.length);
        return result;
    }

    /**
     * Convert a string containing hexadecimal characters to a byte-array.
     *
     * @param s a hex string
     * @return a byte array with the corresponding value
     */
    public static byte[] fromHexString(String s) {
        char[] rawChars = s.toUpperCase().toCharArray();

        int hexChars = 0;
        for (int i = 0; i < rawChars.length; i++) {
            if ((rawChars[i] >= '0' && rawChars[i] <= '9')
                || (rawChars[i] >= 'A' && rawChars[i] <= 'F')) {
                hexChars++;
            }
        }

        byte[] byteString = new byte[(hexChars + 1) >> 1];

        int pos = hexChars & 1;

        for (int i = 0; i < rawChars.length; i++) {
            if (rawChars[i] >= '0' && rawChars[i] <= '9') {
                byteString[pos >> 1] <<= 4;
                byteString[pos >> 1] |= rawChars[i] - '0';
            } else if (rawChars[i] >= 'A' && rawChars[i] <= 'F') {
                byteString[pos >> 1] <<= 4;
                byteString[pos >> 1] |= rawChars[i] - 'A' + 10;
            } else {
                continue;
            }
            pos++;
        }

        return byteString;
    }

    /**
     * Convert a byte array to the corresponding hexstring.
     *
     * @param input the byte array to be converted
     * @return the corresponding hexstring
     */
    public static String toHexString(byte[] input) {
        String result = "";
        for (int i = 0; i < input.length; i++) {
            result += HEX_CHARS[(input[i] >>> 4) & 0x0f];
            result += HEX_CHARS[(input[i]) & 0x0f];
        }
        return result;
    }

    /**
     * Convert a byte array to the corresponding hex string.
     *
     * @param input the byte array to be converted
     * @param prefix the prefix to put at the beginning of the hex string
     * @param seperator a separator string
     * @return the corresponding hex string
     */
    public static String toHexString(byte[] input, String prefix,
        String seperator) {
        String result = new String(prefix);
        for (int i = 0; i < input.length; i++) {
            result += HEX_CHARS[(input[i] >>> 4) & 0x0f];
            result += HEX_CHARS[(input[i]) & 0x0f];
            if (i < input.length - 1) {
                result += seperator;
            }
        }
        return result;
    }

    /**
     * Convert a byte array to the corresponding bit string.
     *
     * @param input the byte array to be converted
     * @return the corresponding bit string
     */
    public static String toBinaryString(byte[] input) {
        String result = "";
        int i;
        for (i = 0; i < input.length; i++) {
            int e = input[i];
            for (int ii = 0; ii < 8; ii++) {
                int b = (e >>> ii) & 1;
                result += b;
            }
            if (i != input.length - 1) {
                result += " ";
            }
        }
        return result;
    }

    /**
     * Compute the bitwise XOR of two arrays of bytes. The arrays have to be of
     * same length. No length checking is performed.
     *
     * @param x1 the first array
     * @param x2 the second array
     * @return x1 XOR x2
     */
    public static byte[] xor(byte[] x1, byte[] x2) {
        byte[] out = new byte[x1.length];

        for (int i = x1.length - 1; i >= 0; i--) {
            out[i] = (byte) (x1[i] ^ x2[i]);
        }
        return out;
    }

    /**
     * Concatenate two byte arrays. No null checks are performed.
     *
     * @param x1 the first array
     * @param x2 the second array
     * @return (x2||x1) (little-endian order, i.e. x1 is at lower memory
     * addresses)
     */
    public static byte[] concatenate(byte[] x1, byte[] x2) {
        byte[] result = new byte[x1.length + x2.length];

        System.arraycopy(x1, 0, result, 0, x1.length);
        System.arraycopy(x2, 0, result, x1.length, x2.length);

        return result;
    }

    /**
     * Convert a 2-dimensional byte array into a 1-dimensional byte array by
     * concatenating all entries.
     *
     * @param array a 2-dimensional byte array
     * @return the concatenated input array
     */
    public static byte[] concatenate(byte[][] array) {
        int rowLength = array[0].length;
        byte[] result = new byte[array.length * rowLength];
        int index = 0;
        for (int i = 0; i < array.length; i++) {
            System.arraycopy(array[i], 0, result, index, rowLength);
            index += rowLength;
        }
        return result;
    }

    /**
     * Split a byte array <tt>input</tt> into two arrays at <tt>index</tt>,
     * i.e. the first array will have the lower <tt>index</tt> bytes, the
     * second one the higher <tt>input.length - index</tt> bytes.
     *
     * @param input the byte array to be split
     * @param index the index where the byte array is split
     * @return the splitted input array as an array of two byte arrays
     * @throws ArrayIndexOutOfBoundsException if <tt>index</tt> is out of bounds
     */
    public static byte[][] split(byte[] input, int index)
        throws ArrayIndexOutOfBoundsException {
        if (index > input.length) {
            throw new ArrayIndexOutOfBoundsException();
        }
        byte[][] result = new byte[2][];
        result[0] = new byte[index];
        result[1] = new byte[input.length - index];
        System.arraycopy(input, 0, result[0], 0, index);
        System.arraycopy(input, index, result[1], 0, input.length - index);
        return result;
    }

    /**
     * Generate a subarray of a given byte array.
     *
     * @param input the input byte array
     * @param start the start index
     * @param end the end index
     * @return a subarray of <tt>input</tt>, ranging from <tt>start</tt>
     * (inclusively) to <tt>end</tt> (exclusively)
     */
    public static byte[] subArray(byte[] input, int start, int end) {
        byte[] result = new byte[end - start];
        System.arraycopy(input, start, result, 0, end - start);
        return result;
    }

    /**
     * Generate a subarray of a given byte array.
     *
     * @param input the input byte array
     * @param start the start index
     * @return a subarray of <tt>input</tt>, ranging from <tt>start</tt> to
     * the end of the array
     */
    public static byte[] subArray(byte[] input, int start) {
        return subArray(input, start, input.length);
    }

    /**
     * Rewrite a byte array as a char array
     *
     * @param input -
     * the byte array
     * @return char array
     */
    public static char[] toCharArray(byte[] input) {
        char[] result = new char[input.length];
        for (int i = 0; i < input.length; i++) {
            result[i] = (char) input[i];
        }
        return result;
    }

}
