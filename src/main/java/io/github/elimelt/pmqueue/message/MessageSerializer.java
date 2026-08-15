package io.github.elimelt.pmqueue.message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A high-performance serializer for {@link Message} objects using
 * {@link ByteBuffer} operations.
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
 * All multi-byte fields are written and read in the JVM's native byte order,
 * matching the on-wire layout this class has always produced.
 *
 * <p>
 * Performance optimizations include:
 * <ul>
 * <li>Thread-local {@link ByteBuffer} reuse to minimize allocation
 * <li>Buffer size doubling strategy for growing buffers
 * </ul>
 *
 * <p>
 * <strong>Note:</strong> This class is not intended for external use and
 * should only be used by the {@link Message} class's serialization mechanism.
 */
public class MessageSerializer {
  private static final int HEADER_SIZE = 16;

  private MessageSerializer() {
  }

  private static final ThreadLocal<ByteBuffer> threadLocalBuffer = ThreadLocal
      .withInitial(() -> ByteBuffer.allocateDirect(4096).order(ByteOrder.nativeOrder()));

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
      buffer = ByteBuffer.allocateDirect(Math.max(totalLength, buffer.capacity() * 2))
          .order(ByteOrder.nativeOrder());
      threadLocalBuffer.set(buffer);
    }

    buffer.clear();
    buffer.putLong(message.getTimestamp());
    buffer.putInt(message.getMessageType());
    buffer.putInt(data.length);
    buffer.put(data);

    byte[] result = new byte[totalLength];
    buffer.flip();
    buffer.get(result);

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
   * extracted from the serialized format using a {@link ByteBuffer} view for
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

    ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
    long timestamp = buffer.getLong(0);
    int type = buffer.getInt(8);
    int length = buffer.getInt(12);

    if (length < 0 || length > bytes.length - HEADER_SIZE) {
      throw new IOException("Invalid message length");
    }

    byte[] data = new byte[length];
    buffer.position(HEADER_SIZE);
    buffer.get(data);

    return new Message(data, type) {
      @Override
      public long getTimestamp() {
        return timestamp;
      }
    };
  }
}
