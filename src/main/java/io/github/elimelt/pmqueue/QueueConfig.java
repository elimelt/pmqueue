package io.github.elimelt.pmqueue;

import io.github.elimelt.pmqueue.core.PersistentMessageQueue;

/**
 * Configuration for a message queue.
 * This class is immutable and thread-safe.
 *
 * Example usage:
 *
 * <pre>{@code
 * QueueConfig config = new QueueConfig.Builder()
 *     .filePath(filePath)
 *     .debugEnabled(true)
 *     .checksumEnabled(true)
 *     .maxFileSize(1024 * 1024 * 1024)
 *     .initialFileSize(4096)
 *     .defaultBufferSize(1024 * 1024)
 *     .maxBufferSize(8 * 1024 * 1024)
 *     .batchThreshold(64)
 *     .build();
 * }</pre>
 */
public class QueueConfig {
  private final String filePath;
  private final boolean debugEnabled;
  private final boolean checksumEnabled;
  private final long maxFileSize;
  private final int initialFileSize;
  private final int defaultBufferSize;
  private final int maxBufferSize;
  private final int batchThreshold;

  // private to enforce Builder
  private QueueConfig(Builder builder) {
    this.filePath = builder.filePath;
    this.debugEnabled = builder.debugEnabled;
    this.checksumEnabled = builder.checksumEnabled;
    this.maxFileSize = builder.maxFileSize;
    this.initialFileSize = builder.initialFileSize;
    this.defaultBufferSize = builder.defaultBufferSize;
    this.maxBufferSize = builder.maxBufferSize;
    this.batchThreshold = builder.batchThreshold;
  }

  /**
   * Builder for QueueConfig.
   * This class is mutable and not thread-safe.
   */
  public static class Builder {
    private String filePath = "queue.dat";
    private boolean debugEnabled = false;
    private boolean checksumEnabled = true;
    private long maxFileSize = 1024L * 1024L * 1024L; // 1GB
    private int initialFileSize = PersistentMessageQueue.QUEUE_HEADER_SIZE;
    private int defaultBufferSize = 1024 * 1024; // 1MB
    private int maxBufferSize = 8 * 1024 * 1024; // 8MB
    private int batchThreshold = 64;

    /**
     * Creates a new Builder.
     */
    public Builder() {
    }

    /**
     * Sets the file path for the message queue.
     *
     * @param filePath the file path
     * @return this
     */
    public Builder filePath(String filePath) {
      this.filePath = filePath;
      return this;
    }

    /**
     * Enables or disables debug mode.
     *
     * @param debugEnabled true to enable debug mode, false otherwise
     * @return this
     */
    public Builder debugEnabled(boolean debugEnabled) {
      this.debugEnabled = debugEnabled;
      return this;
    }

    /**
     * Enables or disables checksums.
     *
     * @param checksumEnabled true to enable checksums, false otherwise
     * @return this
     */
    public Builder checksumEnabled(boolean checksumEnabled) {
      this.checksumEnabled = checksumEnabled;
      return this;
    }

    /**
     * Sets the maximum file size for the message queue.
     *
     * @param maxFileSize the maximum file size
     * @return this
     */
    public Builder maxFileSize(long maxFileSize) {
      this.maxFileSize = maxFileSize;
      return this;
    }

    /**
     * Sets the initial file size for the message queue.
     *
     * @param initialFileSize the initial file size
     * @return this
     */
    public Builder initialFileSize(int initialFileSize) {
      this.initialFileSize = initialFileSize;
      return this;
    }

    /**
     * Sets the default buffer size for the message queue.
     *
     * @param defaultBufferSize the default buffer size
     * @return this
     */
    public Builder defaultBufferSize(int defaultBufferSize) {
      this.defaultBufferSize = defaultBufferSize;
      return this;
    }

    /**
     * Sets the maximum buffer size for the message queue.
     *
     * @param maxBufferSize the maximum buffer size
     * @return this
     */
    public Builder maxBufferSize(int maxBufferSize) {
      this.maxBufferSize = maxBufferSize;
      return this;
    }

    /**
     * Sets the batch threshold for the message queue.
     *
     * @param batchThreshold the batch threshold
     * @return this
     */
    public Builder batchThreshold(int batchThreshold) {
      this.batchThreshold = batchThreshold;
      return this;
    }

    /**
     * Builds the QueueConfig.
     *
     * @return the QueueConfig
     */
    public QueueConfig build() {
      // validate configuration
      if (maxBufferSize < defaultBufferSize) {
        throw new IllegalArgumentException("maxBufferSize must be >= defaultBufferSize");
      }
      if (initialFileSize < PersistentMessageQueue.QUEUE_HEADER_SIZE) {
        throw new IllegalArgumentException("initialFileSize must be >= " + PersistentMessageQueue.QUEUE_HEADER_SIZE);
      }
      if (batchThreshold <= 0) {
        throw new IllegalArgumentException("batchThreshold must be > 0");
      }
      return new QueueConfig(this);
    }
  }

  /**
   * Returns the file path for the message queue.
   *
   * @return the file path
   */
  public String getFilePath() {
    return filePath;
  }

  /**
   * Returns whether debug mode is enabled.
   *
   * @return true if debug mode is enabled, false otherwise
   */
  public boolean isDebugEnabled() {
    return debugEnabled;
  }

  /**
   * Returns whether checksums are enabled.
   *
   * @return true if checksums are enabled, false otherwise
   */
  public boolean isChecksumEnabled() {
    return checksumEnabled;
  }

  /**
   * Returns the maximum file size for the message queue.
   *
   * @return the maximum file size
   */
  public long getMaxFileSize() {
    return maxFileSize;
  }

  /**
   * Returns the initial file size for the message queue.
   *
   * @return the initial file size
   */
  public int getInitialFileSize() {
    return initialFileSize;
  }

  /**
   * Returns the default buffer size for the message queue.
   *
   * @return the default buffer size
   */
  public int getDefaultBufferSize() {
    return defaultBufferSize;
  }

  /**
   * Returns the maximum buffer size for the message queue.
   *
   * @return the maximum buffer size
   */
  public int getMaxBufferSize() {
    return maxBufferSize;
  }

  /**
   * Returns the batch threshold for the message queue.
   *
   * @return the batch threshold
   */
  public int getBatchThreshold() {
    return batchThreshold;
  }
}