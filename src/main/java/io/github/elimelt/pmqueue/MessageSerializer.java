package io.github.elimelt.pmqueue;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import sun.misc.Unsafe;
import java.lang.reflect.Field;

@SuppressWarnings("deprecation")
class MessageSerializer {
    private static final int HEADER_SIZE = 16; // 8 bytes timestamp + 4 bytes type + 4 bytes length

    // Thread-local ByteBuffer for reuse
    private static final ThreadLocal<ByteBuffer> threadLocalBuffer =
        ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(4096));

    // Unsafe instance for direct memory operations
    private static final Unsafe unsafe;

    // Base address of direct ByteBuffer for optimized access
    private static final long addressOffset;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);

            Field addressField = Buffer.class.getDeclaredField("address");
            addressOffset = unsafe.objectFieldOffset(addressField);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    public static byte[] serialize(Message message) throws IOException {
        if (message == null) {
            throw new IOException("Message is null");
        }

        byte[] data = message.getData();
        int totalLength = HEADER_SIZE + data.length;

        // Get thread-local buffer or allocate new if needed
        ByteBuffer buffer = threadLocalBuffer.get();
        if (buffer.capacity() < totalLength) {
            buffer = ByteBuffer.allocateDirect(Math.max(totalLength, buffer.capacity() * 2));
            threadLocalBuffer.set(buffer);
        }

        buffer.clear();
        long bufferAddress = unsafe.getLong(buffer, addressOffset);

        // Direct memory writes
        unsafe.putLong(bufferAddress, message.getTimestamp());
        unsafe.putInt(bufferAddress + 8, message.getMessageType());
        unsafe.putInt(bufferAddress + 12, data.length);
        unsafe.copyMemory(data, Unsafe.ARRAY_BYTE_BASE_OFFSET,
                         null, bufferAddress + HEADER_SIZE,
                         data.length);

        // Create result array and copy data
        byte[] result = new byte[totalLength];
        unsafe.copyMemory(null, bufferAddress,
                         result, Unsafe.ARRAY_BYTE_BASE_OFFSET,
                         totalLength);

        return result;
    }

    public static Message deserialize(byte[] bytes) throws IOException {
        if (bytes.length < HEADER_SIZE) {
            throw new IOException("Invalid message: too short");
        }

        // Direct memory reads using Unsafe
        long timestamp = unsafe.getLong(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET);
        int type = unsafe.getInt(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET + 8);
        int length = unsafe.getInt(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET + 12);

        if (length < 0 || length > bytes.length - HEADER_SIZE) {
            throw new IOException("Invalid message length");
        }

        // Create data array and copy directly
        byte[] data = new byte[length];
        unsafe.copyMemory(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET + HEADER_SIZE,
                         data, Unsafe.ARRAY_BYTE_BASE_OFFSET,
                         length);

        return new Message(data, type) {
            @Override
            public long getTimestamp() {
                return timestamp;
            }
        };
    }
}