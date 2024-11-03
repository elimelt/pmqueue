package io.github.elimelt.pmqueue;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import sun.misc.Unsafe;
import java.lang.reflect.Field;

/**
 * A high-performance serializer for {@link Message} objects using direct memory
 * operations.
 * This class provides methods to convert {@link Message} objects to and from
 * byte arrays
 * with minimal overhead and maximum performance.
 *
 * <p>
 * The serialization format consists of:
 * <ul>
 * <li>8 bytes: timestamp (long)
 * <li>4 bytes: message type (int)
 * <li>4 bytes: data length (int)
 * <li>n bytes: message data
 * </ul>
 *
 * <p>
 * Performance optimizations include:
 * <ul>
 * <li>Thread-local {@link ByteBuffer} reuse to minimize allocation
 * <li>Direct memory operations using {@link sun.misc.Unsafe}
 * <li>Buffer size doubling strategy for growing buffers
 * </ul>
 *
 * <p>
 * <strong>Note:</strong> This class is not intended for external use and
 * should only be used by the {@link Message} class's serialization mechanism.
 */
@SuppressWarnings("deprecation")
class MessageSerializer {
  private static final int HEADER_SIZE = 16;

  private static final ThreadLocal<ByteBuffer> threadLocalBuffer = ThreadLocal
      .withInitial(() -> ByteBuffer.allocateDirect(4096));

  private static final Unsafe unsafe;
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

  /**
   * Serializes a {@link Message} object into a byte array.
   * The resulting byte array contains the message's timestamp, type, length,
   * and data in a compact binary format.
   *
   * <p>
   * This method uses thread-local direct {@link ByteBuffer}s to optimize
   * performance and minimize garbage collection pressure. The buffer size
   * automatically grows if needed.
   *
   * @param message the Message object to serialize
   * @return a byte array containing the serialized message
   * @throws IOException      if the message is null or cannot be serialized
   * @throws OutOfMemoryError if unable to allocate required buffer space
   */
  public static byte[] serialize(Message message) throws IOException {
    if (message == null) {
      throw new IOException("Message is null");
    }

    byte[] data = message.getData();
    int totalLength = HEADER_SIZE + data.length;

    ByteBuffer buffer = threadLocalBuffer.get();
    if (buffer.capacity() < totalLength) {
      buffer = ByteBuffer.allocateDirect(Math.max(totalLength, buffer.capacity() * 2));
      threadLocalBuffer.set(buffer);
    }

    buffer.clear();
    long bufferAddress = unsafe.getLong(buffer, addressOffset);

    unsafe.putLong(bufferAddress, message.getTimestamp());
    unsafe.putInt(bufferAddress + 8, message.getMessageType());
    unsafe.putInt(bufferAddress + 12, data.length);
    unsafe.copyMemory(data, Unsafe.ARRAY_BYTE_BASE_OFFSET,
        null, bufferAddress + HEADER_SIZE,
        data.length);

    byte[] result = new byte[totalLength];
    unsafe.copyMemory(null, bufferAddress,
        result, Unsafe.ARRAY_BYTE_BASE_OFFSET,
        totalLength);

    return result;
  }

  /**
   * Deserializes a byte array into a {@link Message} object.
   * The byte array must contain data in the format produced by
   * {@link #serialize}.
   *
   * <p>
   * This method creates a new Message object with the original timestamp
   * preserved through anonymous subclassing. The message type and data are
   * extracted from the serialized format using direct memory operations for
   * optimal performance.
   *
   * @param bytes the byte array containing the serialized message
   * @return a new Message object with the deserialized data
   * @throws IOException if the byte array is too short, contains invalid length
   *                     information, or is otherwise malformed
   */
  public static Message deserialize(byte[] bytes) throws IOException {
    if (bytes.length < HEADER_SIZE) {
      throw new IOException("Invalid message: too short");
    }

    long timestamp = unsafe.getLong(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET);
    int type = unsafe.getInt(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET + 8);
    int length = unsafe.getInt(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET + 12);

    if (length < 0 || length > bytes.length - HEADER_SIZE) {
      throw new IOException("Invalid message length");
    }

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