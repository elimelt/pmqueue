package io.github.elimelt.pmqueue;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.ref.SoftReference;
import sun.misc.Unsafe;
import java.lang.reflect.Field;

/**
 * A high-performance, immutable message container optimized for memory
 * efficiency and fast access.
 * This class uses direct memory operations via {@link sun.misc.Unsafe} for
 * improved performance
 * and implements custom serialization for better control over the serialization
 * process.
 *
 * <p>
 * The message contains:
 * <ul>
 * <li>A byte array containing the message data
 * <li>A timestamp recording when the message was created
 * <li>A message type identifier
 * </ul>
 *
 * <p>
 * This class implements optimizations including:
 * <ul>
 * <li>Direct memory access using Unsafe for field operations
 * <li>Cached hash code using soft references to allow GC if memory is tight
 * <li>Custom serialization implementation for performance
 * </ul>
 *
 * @see MessageSerializer
 */
@SuppressWarnings("deprecation")
public class Message implements Serializable {
  private static final long serialVersionUID = 1L;

  private transient SoftReference<Integer> hashCache;
  private final byte[] data;
  private final long timestamp;
  private final int messageType;
  private final int length;

  private static final Unsafe unsafe;
  @SuppressWarnings("unused")
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

  /**
   * Creates a new Message with the specified data and message type.
   * The message's timestamp is automatically set to the current system time.
   * A defensive copy of the input data is made to ensure immutability.
   *
   * @param data        the byte array containing the message data
   * @param messageType an integer identifying the type of message
   * @throws NullPointerException if data is null
   */
  public Message(byte[] data, int messageType) {
    int dataLength = data.length;
    this.data = new byte[dataLength];
    unsafe.copyMemory(data, Unsafe.ARRAY_BYTE_BASE_OFFSET,
        this.data, Unsafe.ARRAY_BYTE_BASE_OFFSET,
        dataLength);
    this.length = dataLength;
    this.timestamp = System.currentTimeMillis();
    this.messageType = messageType;
  }

  /**
   * Returns a copy of the message data.
   * A new array is created and returned each time to preserve immutability.
   *
   * @return a copy of the message data as a byte array
   */
  public byte[] getData() {
    byte[] copy = new byte[length];
    unsafe.copyMemory(data, Unsafe.ARRAY_BYTE_BASE_OFFSET,
        copy, Unsafe.ARRAY_BYTE_BASE_OFFSET,
        length);
    return copy;
  }

  /**
   * Returns the timestamp when this message was created.
   *
   * @return the message creation timestamp as milliseconds since epoch
   */
  public long getTimestamp() {
    return unsafe.getLong(this, timestampOffset);
  }

  /**
   * Returns the message type identifier.
   *
   * @return the integer message type
   */
  public int getMessageType() {
    return unsafe.getInt(this, messageTypeOffset);
  }

  /**
   * Computes and caches the hash code for this message using the FNV-1a
   * algorithm.
   * The hash is computed based on the message data, timestamp, and message type.
   * The computed hash is cached using a {@link SoftReference} to allow garbage
   * collection
   * if memory is tight.
   *
   * @return the hash code for this message
   */
  @Override
  public int hashCode() {
    Integer cachedHash = hashCache != null ? hashCache.get() : null;
    if (cachedHash != null) {
      return cachedHash;
    }

    int hash = 0x811c9dc5;
    for (byte b : data) {
      hash ^= b;
      hash *= 0x01000193;
    }
    hash = hash * 31 + (int) (timestamp ^ (timestamp >>> 32));
    hash = hash * 31 + messageType;

    hashCache = new SoftReference<>(hash);
    return hash;
  }

  /**
   * Custom serialization implementation for better performance.
   * Writes the message fields directly to the output stream.
   *
   * @param out the output stream to write to
   * @throws IOException if an I/O error occurs
   */
  private void writeObject(ObjectOutputStream out) throws IOException {
    out.writeLong(unsafe.getLong(this, timestampOffset));
    out.writeInt(unsafe.getInt(this, messageTypeOffset));
    out.writeInt(length);
    out.write(data, 0, length);
  }

  /**
   * Disabled default deserialization.
   * Use {@link MessageSerializer} instead for proper deserialization.
   *
   * @param in the input stream to read from
   * @throws IOException always, to prevent default deserialization
   */
  private void readObject(ObjectInputStream in) throws IOException {
    throw new IOException("Use MessageSerializer instead");
  }
}