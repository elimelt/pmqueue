package io.github.elimelt.pmqueue;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.ref.SoftReference;
import java.util.Arrays;
import sun.misc.Unsafe;
import java.lang.reflect.Field;

public class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    // Use SoftReference for the cached hash to allow GC if memory is tight
    private transient SoftReference<Integer> hashCache;

    // Direct byte array reference for minimal overhead
    private final byte[] data;
    private final long timestamp;
    private final int messageType;

    // Cache array length to avoid field access
    private final int length;

    // Unsafe instance for direct memory operations
    private static final Unsafe unsafe;

    // Field offsets for direct memory access
    private static final long dataOffset;
    private static final long timestampOffset;
    private static final long messageTypeOffset;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);

            dataOffset = unsafe.objectFieldOffset(Message.class.getDeclaredField("data"));
            timestampOffset = unsafe.objectFieldOffset(Message.class.getDeclaredField("timestamp"));
            messageTypeOffset = unsafe.objectFieldOffset(Message.class.getDeclaredField("messageType"));
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    public Message(byte[] data, int messageType) {
        // Avoid double array length access
        int dataLength = data.length;
        this.data = new byte[dataLength];
        // Direct memory copy instead of Arrays.copyOf
        unsafe.copyMemory(data, Unsafe.ARRAY_BYTE_BASE_OFFSET,
                         this.data, Unsafe.ARRAY_BYTE_BASE_OFFSET,
                         dataLength);
        this.length = dataLength;
        this.timestamp = System.currentTimeMillis();
        this.messageType = messageType;
    }

    public byte[] getData() {
        byte[] copy = new byte[length];
        // Direct memory copy for better performance
        unsafe.copyMemory(data, Unsafe.ARRAY_BYTE_BASE_OFFSET,
                         copy, Unsafe.ARRAY_BYTE_BASE_OFFSET,
                         length);
        return copy;
    }

    public long getTimestamp() {
        // Direct memory access instead of field access
        return unsafe.getLong(this, timestampOffset);
    }

    public int getMessageType() {
        // Direct memory access instead of field access
        return unsafe.getInt(this, messageTypeOffset);
    }

    @Override
    public int hashCode() {
        Integer cachedHash = hashCache != null ? hashCache.get() : null;
        if (cachedHash != null) {
            return cachedHash;
        }

        // FNV-1a hash algorithm - faster than Arrays.hashCode
        int hash = 0x811c9dc5;
        for (byte b : data) {
            hash ^= b;
            hash *= 0x01000193;
        }
        hash = hash * 31 + (int)(timestamp ^ (timestamp >>> 32));
        hash = hash * 31 + messageType;

        hashCache = new SoftReference<>(hash);
        return hash;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        // Direct field access for better performance
        out.writeLong(unsafe.getLong(this, timestampOffset));
        out.writeInt(unsafe.getInt(this, messageTypeOffset));
        out.writeInt(length);
        out.write(data, 0, length);
    }

    private void readObject(ObjectInputStream in) throws IOException {
        throw new IOException("Use MessageSerializer instead");
    }
}