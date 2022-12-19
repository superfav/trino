/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.parquet.reader;

import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Basic input stream based on a given Slice object.
 * This is a simpler version of BasicSliceInput with a few additional methods.
 * <p>
 * Note that methods starting with 'read' modify the underlying offset, while 'get' methods return
 * value without modifying the state
 */
public final class SimpleSliceInputStream
{
    private final Slice slice;
    private int offset;

    public SimpleSliceInputStream(Slice slice)
    {
        this(slice, 0);
    }

    public SimpleSliceInputStream(Slice slice, int offset)
    {
        this.slice = requireNonNull(slice, "slice is null");
        checkArgument(slice.length() == 0 || slice.hasByteArray(), "SimpleSliceInputStream supports only slices backed by byte array");
        this.offset = offset;
    }

    public byte readByte()
    {
        return slice.getByte(offset++);
    }

    public long readLong()
    {
        long value = slice.getLong(offset);
        offset += Long.BYTES;
        return value;
    }

    public byte[] readBytes()
    {
        byte[] bytes = slice.getBytes();
        offset = slice.length();
        return bytes;
    }

    public void skip(int n)
    {
        offset += n;
    }

    public Slice asSlice()
    {
        return slice.slice(offset, slice.length() - offset);
    }

    /**
     * Returns the byte array wrapped by this Slice.
     * Callers should take care to use {@link SimpleSliceInputStream#getByteArrayOffset()}
     * since the contents of this Slice may not start at array index 0.
     */
    public byte[] getByteArray()
    {
        return slice.byteArray();
    }

    /**
     * Returns the start index the content of this slice within the byte array wrapped by this slice.
     */
    public int getByteArrayOffset()
    {
        return offset + slice.byteArrayOffset();
    }
}
