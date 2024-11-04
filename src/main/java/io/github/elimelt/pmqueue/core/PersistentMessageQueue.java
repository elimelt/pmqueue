package io.github.elimelt.pmqueue.core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

import io.github.elimelt.pmqueue.MessageQueue;
import io.github.elimelt.pmqueue.QueueConfig;
import io.github.elimelt.pmqueue.message.Message;
import io.github.elimelt.pmqueue.message.MessageSerializer;

/**
 * A high-performance, persistent queue implementation for storing messages on
 * disk.
 * This queue provides durability guarantees while maintaining efficient read
 * and write
 * operations through various optimizations including write batching and direct
 * I/O.
 *
 * <p>
 * The queue stores messages in a single file with the following structure:
 * <ul>
 * <li>Queue Header (24 bytes):
 * <ul>
 * <li>Front offset (8 bytes)
 * <li>Rear offset (8 bytes)
 * <li>Reserved (8 bytes)
 * </ul>
 * <li>Message Blocks:
 * <ul>
 * <li>Block Header (8 bytes):
 * <ul>
 * <li>Message size (4 bytes)
 * <li>CRC32 checksum (4 bytes)
 * </ul>
 * <li>Message data (variable size)
 * </ul>
 * </ul>
 *
 * <p>
 * Key features:
 * <ul>
 * <li>Write batching for improved performance
 * <li>Direct ByteBuffers for efficient I/O
 * <li>CRC32 checksums for data integrity
 * <li>Fair locking for predictable ordering
 * <li>Automatic file size management
 * <li>Thread-safe operations
 * </ul>
 *
 * <p>
 * <b>Example usage:</b>
 * {@code
 * // Create a new queue
 * try (PersistentMessageQueue queue = new PersistentMessageQueue("messages.queue")) {
 *     // Create and offer a message
 *     Message msg1 = new Message("Hello".getBytes(), 1);
 *     queue.offer(msg1);
 *
 *     // Poll a message from the queue
 *     Message received = queue.poll();
 *     if (received != null) {
 *         System.out.println(new String(received.getData()));
 * }
 * }
 * }
 *
 * <p>
 * <b>Performance considerations:</b>
 * <ul>
 * <li>Small messages are batched together for better throughput
 * <li>Large messages are written directly to disk
 * <li>File size grows dynamically but is limited to 1GB
 * <li>Default buffer size is 1MB, aligned to page boundaries
 * </ul>
 */
public class PersistentMessageQueue implements MessageQueue {
  /*
   * File format:
   * +-----------------+-----------------+-----------------+
   * | Front offset | Rear offset | Reserved |
   * +-----------------+-----------------+-----------------+
   * | Message size | CRC32 checksum | Message data |
   * +-----------------+-----------------+-----------------+
   * | ... | ... | ... |
   * +-----------------+-----------------+-----------------+
   * | Message size | CRC32 checksum | Message data |
   * +-----------------+-----------------+-----------------+
   * | ... | ... | ... |
   * +-----------------+-----------------+-----------------+
   * | ... | ... | ... |
   * +-----------------+-----------------+-----------------+
   */

  /**
   * The size of the queue header in bytes.
   */
  public static final int QUEUE_HEADER_SIZE = 24;

  /**
   * The size of the block header in bytes.
   */
  public static final int BLOCK_HEADER_SIZE = 8;

  /**
   * The size of a page in bytes.
   */
  public static final int PAGE_SIZE = 4096;

  // default configuration
  private static boolean debug = false;
  private static boolean shouldChecksum = true;
  private static long maxFileSize = 1024L * 1024L * 1024L; // 1GB
  private static int initialFileSize = QUEUE_HEADER_SIZE;
  private static int defaultBufferSize = (1024 * 1024 / PAGE_SIZE) * PAGE_SIZE;
  private static int maxBufferSize = (8 * 1024 * 1024 / PAGE_SIZE) * PAGE_SIZE;
  private static int batchThreshold = 64;

  // instance variables
  private final ByteBuffer writeBatchBuffer;
  private int batchSize = 0;
  private long batchStartOffset;
  private final FileChannel channel;
  private final RandomAccessFile file;
  private ByteBuffer messageBuffer;
  private volatile long frontOffset;
  private volatile long rearOffset;
  private final ReentrantLock lock;
  private final CRC32 checksumCalculator;

  /**
   * Creates a new persistent message queue with custom configuration.
   *
   * @param config the configuration for the queue
   * @throws IOException       if the file cannot be created/opened or if the
   *                           existing file is corrupted
   * @throws SecurityException if the application doesn't have required file
   *                           permissions
   */
  public PersistentMessageQueue(QueueConfig config) throws IOException {
    // configure
    debug = config.isDebugEnabled();
    shouldChecksum = config.isChecksumEnabled();
    maxFileSize = config.getMaxFileSize();
    initialFileSize = config.getInitialFileSize();
    defaultBufferSize = alignToPageSize(config.getDefaultBufferSize());
    maxBufferSize = alignToPageSize(config.getMaxBufferSize());
    batchThreshold = config.getBatchThreshold();

    // init queue
    File f = new File(config.getFilePath());
    boolean isNew = !f.exists();
    this.file = new RandomAccessFile(f, "rw");
    this.channel = file.getChannel();

    this.messageBuffer = ByteBuffer.allocateDirect(defaultBufferSize);
    this.writeBatchBuffer = ByteBuffer.allocateDirect(maxBufferSize);
    this.lock = new ReentrantLock(true);

    this.checksumCalculator = shouldChecksum ? new CRC32() : null;

    if (isNew) {
      initializeNewFile();
    } else {
      loadMetadata();
    }
  }

  /**
   * Creates a new persistent message queue with default configuration.
   * This constructor is maintained for backward compatibility.
   *
   * @param filename the path to the queue file
   * @throws IOException       if the file cannot be created/opened or if the
   *                           existing file is corrupted
   * @throws SecurityException if the application doesn't have required file
   *                           permissions
   */
  public PersistentMessageQueue(String filename) throws IOException {
    this(new QueueConfig.Builder().filePath(filename).build());
  }

  // align buffer sizes to page size
  private static int alignToPageSize(int size) {
    return (size / PAGE_SIZE) * PAGE_SIZE;
  }

  /**
   * Offers a message to the queue.
   *
   * <p>
   * Messages smaller than 256KB are batched together for better performance.
   * Larger messages are written directly to disk. If the queue file would exceed
   * its maximum size (1GB), the message is rejected.
   *
   * <p>
   * <b>Example:</b>
   * {@code
   * Message msg = new Message("Important data".getBytes(), 1);
   * boolean success = queue.offer(msg);
   * if (!success) {
   *     System.err.println("Queue is full");
   * }
   * }
   *
   * @param message the message to add to the queue
   * @return true if the message was added, false if the queue is full
   * @throws IOException          if an I/O error occurs
   * @throws NullPointerException if message is null
   */
  public boolean offer(Message message) throws IOException {
    if (message == null)
      throw new NullPointerException("Message cannot be null");

    byte[] serialized = MessageSerializer.serialize(message);
    int totalSize = BLOCK_HEADER_SIZE + serialized.length;

    lock.lock();
    try {
      if (rearOffset + totalSize > maxFileSize)
        return false;

      long requiredLength = rearOffset + totalSize;
      if (requiredLength > file.length()) {
        long newSize = Math.min(maxFileSize,
            Math.max(file.length() * 2, requiredLength + defaultBufferSize));
        file.setLength(newSize);
      }
      if (shouldChecksum) {
        checksumCalculator.reset();
        checksumCalculator.update(serialized);
      }

      int checksum = shouldChecksum ? (int) checksumCalculator.getValue() : 0;

      if (batchSize == 0) {
        batchStartOffset = rearOffset;
      }

      if (serialized.length < defaultBufferSize / 4 &&
          totalSize <= maxBufferSize - writeBatchBuffer.position() &&
          batchSize < batchThreshold) {

        writeBatchBuffer.putInt(serialized.length);
        writeBatchBuffer.putInt(checksum);
        writeBatchBuffer.put(serialized);
        batchSize++;

        rearOffset += totalSize;

        if (batchSize >= batchThreshold ||
            writeBatchBuffer.position() >= writeBatchBuffer.capacity() / 2) {
          flushBatch();
        }
      } else {
        if (batchSize > 0) {
          flushBatch();
        }

        if (serialized.length + BLOCK_HEADER_SIZE > messageBuffer.capacity()) {
          messageBuffer = ByteBuffer.allocateDirect(
              Math.min(maxBufferSize, serialized.length + BLOCK_HEADER_SIZE));
        }

        messageBuffer.clear();
        messageBuffer.putInt(serialized.length);
        messageBuffer.putInt(checksum);
        messageBuffer.put(serialized);
        messageBuffer.flip();

        channel.write(messageBuffer, rearOffset);
        rearOffset += totalSize;
        saveMetadata();
      }

      return true;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Retrieves and removes the head of the queue, or returns null if the queue is
   * empty.
   *
   * <p>
   * This operation ensures data integrity by validating the CRC32 checksum of the
   * message before returning it.
   *
   * <p>
   * <b>Example:</b>
   * {@code
   * while (true) {
   *     Message msg = queue.poll();
   *     if (msg == null) {
   *         // Queue is empty
   *         break;
   * }
   * processMessage(msg);
   * }
   * }
   *
   * @return the head message of the queue, or null if the queue is empty
   * @throws IOException if an I/O error occurs or if the message data is
   *                     corrupted
   */
  public Message poll() throws IOException {
    lock.lock();
    try {
      if (isEmpty())
        return null;

      if (batchSize > 0) {
        flushBatch();
      }

      ByteBuffer headerBuffer = ByteBuffer.allocate(BLOCK_HEADER_SIZE);
      int bytesRead = channel.read(headerBuffer, frontOffset);
      if (bytesRead != BLOCK_HEADER_SIZE) {
        throw new IOException("Failed to read message header");
      }
      headerBuffer.flip();

      int messageSize = headerBuffer.getInt();
      int storedChecksum = headerBuffer.getInt();

      if (messageSize <= 0 || frontOffset + BLOCK_HEADER_SIZE + messageSize > file.length()) {
        throw new IOException(String.format(
            "Corrupted queue: invalid block size %d at offset %d (file length: %d)",
            messageSize, frontOffset, file.length()));
      }

      ByteBuffer dataBuffer = ByteBuffer.allocate(messageSize);
      bytesRead = channel.read(dataBuffer, frontOffset + BLOCK_HEADER_SIZE);
      if (bytesRead != messageSize) {
        throw new IOException("Failed to read message data");
      }
      dataBuffer.flip();

      byte[] data = new byte[messageSize];
      dataBuffer.get(data);
      if (shouldChecksum) {
        checksumCalculator.reset();
        checksumCalculator.update(data);
      }

      int calculatedChecksum = shouldChecksum ? (int) checksumCalculator.getValue() : 0;

      if (storedChecksum != calculatedChecksum) {
        throw new IOException(String.format(
            "Corrupted message: checksum mismatch at offset %d. Expected: %d, Got: %d",
            frontOffset, storedChecksum, calculatedChecksum));
      }

      Message message = MessageSerializer.deserialize(data);
      frontOffset += BLOCK_HEADER_SIZE + messageSize;
      saveMetadata();

      return message;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Checks if the queue is empty.
   *
   * @return true if the queue contains no messages, false otherwise
   */
  public boolean isEmpty() {
    return frontOffset >= rearOffset && batchSize == 0;
  }

  /**
   * Closes the queue, ensuring all pending writes are flushed to disk.
   *
   * <p>
   * This method should be called when the queue is no longer needed to
   * ensure proper resource cleanup. It's recommended to use try-with-resources
   * to ensure the queue is properly closed.
   *
   * @throws IOException if an I/O error occurs while closing
   */
  @Override
  public void close() throws IOException {
    lock.lock();
    try {
      flushBatch();
      saveMetadata();
      channel.force(true);
      channel.close();
      file.close();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Flushes the current write batch to disk.
   *
   * @throws IOException if an I/O error occurs
   */
  public void flushBatch() throws IOException {
    if (batchSize > 0) {
      writeBatchBuffer.flip();
      channel.write(writeBatchBuffer, batchStartOffset);
      writeBatchBuffer.clear();
      batchSize = 0;
      saveMetadata();
    }
  }

  private void loadMetadata() throws IOException {
    if (file.length() < QUEUE_HEADER_SIZE) {
      throw new IOException("File too small to contain valid header");
    }

    ByteBuffer buffer = ByteBuffer.allocate(QUEUE_HEADER_SIZE);
    int bytesRead = channel.read(buffer, 0);
    if (bytesRead != QUEUE_HEADER_SIZE) {
      throw new IOException("Failed to read queue metadata");
    }

    buffer.flip();

    frontOffset = buffer.getLong();
    rearOffset = buffer.getLong();

    if (frontOffset < QUEUE_HEADER_SIZE || rearOffset < QUEUE_HEADER_SIZE ||
        frontOffset > file.length() || rearOffset > file.length() ||
        frontOffset > rearOffset) {
      throw new IOException("Corrupted queue metadata");
    }
  }

  private void saveMetadata() throws IOException {
    // check for closed file
    if (channel == null || !channel.isOpen() || file == null || !file.getFD().valid() ||
        file.getChannel() == null) {
      throw new IOException("Queue file is closed");
    }

    // check for corruption before writing
    if (frontOffset < QUEUE_HEADER_SIZE || rearOffset < QUEUE_HEADER_SIZE ||
        frontOffset > file.length() || rearOffset > file.length() ||
        frontOffset > rearOffset) {
      throw new IOException("Corrupted queue metadata");
    }
    ByteBuffer buffer = ByteBuffer.allocate(QUEUE_HEADER_SIZE);
    buffer.putLong(frontOffset);
    buffer.putLong(rearOffset);
    buffer.flip();

    channel.write(buffer, 0);
    channel.force(true);
  }

  private void initializeNewFile() throws IOException {
    file.setLength(initialFileSize);
    frontOffset = QUEUE_HEADER_SIZE;
    rearOffset = QUEUE_HEADER_SIZE;
    saveMetadata();
  }

  @SuppressWarnings("unused")
  private void debug(String format, Object... args) {
    if (debug) {
      System.out.printf("[DEBUG] " + format + "%n", args);
    }
  }
}