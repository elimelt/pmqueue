package io.github.elimelt.pmqueue.message;

import java.lang.ref.SoftReference;
import java.util.Arrays;

/**
 * A high-performance, immutable message container optimized for memory
 * efficiency and fast access.
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
 * <li>Cached hash code using soft references to allow GC if memory is tight
 * </ul>
 *
 * @see MessageSerializer
 */
public class Message {

  /**
   * Soft reference to cache the hash code for this message.
   */
  private transient SoftReference<Integer> hashCache;
  /**
   * The message data.
   */
  private final byte[] data;
  /**
   * The timestamp when this message was created.
   */
  private final long timestamp;
  /**
   * The message type identifier.
   */
  private final int messageType;
  /**
   * The length of the message data.
   */
  private final int length;

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
    if (data == null) {
      throw new NullPointerException("Message data cannot be null");
    }
    this.data = data.clone();
    this.length = this.data.length;
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
    return Arrays.copyOf(data, length);
  }

  /**
   * Returns the timestamp when this message was created.
   *
   * @return the message creation timestamp as milliseconds since epoch
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Returns the message type identifier.
   *
   * @return the integer message type
   */
  public int getMessageType() {
    return messageType;
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
   * Compares this message to another object for equality.
   * Two messages are equal if they have the same data, timestamp, and message
   * type, i.e. the same fields used to compute {@link #hashCode()}.
   *
   * @param obj the object to compare against
   * @return true if the given object is a Message with the same data,
   *         timestamp, and message type
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Message)) {
      return false;
    }
    Message other = (Message) obj;
    return timestamp == other.timestamp
        && messageType == other.messageType
        && Arrays.equals(data, other.data);
  }
}
