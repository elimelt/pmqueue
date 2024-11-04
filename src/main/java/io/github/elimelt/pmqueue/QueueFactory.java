package io.github.elimelt.pmqueue;

import java.io.IOException;
import java.util.logging.Logger;

import io.github.elimelt.pmqueue.core.PersistentMessageQueue;

/**
 * Factory for creating MessageQueue instances with various configurations.
 * This class is thread-safe.
 *
 * Example usage:
 *
 * <pre>{@code
 * String filePath = "path/to/queue.dat";
 * MessageQueue queue = QueueFactory.createQueue(filePath);
 * }</pre>
 *
 * Example usage with custom configuration:
 *
 * <pre>{@code
 * String filePath = "path/to/queue.dat";
 * MessageQueue queue = new QueueFactory.CustomQueueBuilder()
 *         .withFilePath(filePath)
 *         .withDebugEnabled(true)
 *         .withChecksumEnabled(true)
 *         .withMaxFileSize(1024 * 1024 * 1024)
 *         .withDefaultBufferSize(1024 * 1024)
 *         .withMaxBufferSize(8 * 1024 * 1024)
 *         .withBatchThreshold(64)
 *         .build();
 * }</pre>
 */
public class QueueFactory {
    @SuppressWarnings("unused")
    private static final Logger logger = Logger.getLogger(QueueFactory.class.getName());

    // prevent instantiation
    private QueueFactory() {
    }

    /**
     * Default queue configuration suitable for general use.
     * Features moderate buffer sizes and checksum verification.
     *
     * @param filePath the file path for the queue
     * @return MessageQueue instance
     * @throws IOException if an I/O error occurs
     */
    public static MessageQueue createQueue(String filePath) throws IOException {
        return new PersistentMessageQueue(
                new QueueConfig.Builder()
                        .filePath(filePath)
                        .build());
    }

    /**
     * Creates a queue optimized for high throughput scenarios.
     * Features larger buffers, batching, and less frequent checksum verification.
     *
     * <p>
     * Configuration:
     * <ul>
     * <li>Default buffer size: 4MB</li>
     * <li>Max buffer size: 16MB</li>
     * <li>Batch threshold: 256</li>
     * <li>Checksums: Disabled</li>
     * </ul>
     *
     * @param filePath the file path for the queue
     * @return MessageQueue instance
     * @throws IOException if an I/O error occurs
     */
    public static MessageQueue createHighThroughputQueue(String filePath) throws IOException {
        return new PersistentMessageQueue(
                new QueueConfig.Builder()
                        .filePath(filePath)
                        .defaultBufferSize(4 * 1024 * 1024) // 4MB default buffer
                        .maxBufferSize(16 * 1024 * 1024) // 16MB max buffer
                        .batchThreshold(256) // Larger batch size
                        .checksumEnabled(false) // Disable checksums for performance
                        .build());
    }

    /**
     * Creates a queue optimized for durability and reliability.
     * Features smaller buffers, checksum verification, and debug logging.
     * <p>
     * Configuration:
     * <ul>
     * <li>Default buffer size: 1MB</li>
     * <li>Max buffer size: 4MB</li>
     * <li>Batch threshold: 32</li>
     * <li>Checksums: Enabled</li>
     * <li>Debug logging: Enabled</li>
     * </ul>
     *
     * @param filePath the file path for the queue
     * @return MessageQueue instance
     * @throws IOException if an I/O error occurs
     */
    public static MessageQueue createDurableQueue(String filePath) throws IOException {
        return new PersistentMessageQueue(
                new QueueConfig.Builder()
                        .filePath(filePath)
                        .defaultBufferSize(1024 * 1024) // 1MB default buffer
                        .maxBufferSize(4 * 1024 * 1024) // 4MB max buffer
                        .batchThreshold(32) // Smaller batch size
                        .checksumEnabled(true) // Enable checksums
                        .debugEnabled(true) // Enable debug logging
                        .build());
    }

    /**
     * Creates a queue optimized for storing large messages.
     * Features larger buffers and smaller batch sizes.
     * <p>
     * Configuration:
     * <ul>
     * <li>Default buffer size: 16MB</li>
     * <li>Max buffer size: 32MB</li>
     * <li>Batch threshold: 16</li>
     * </ul>
     *
     * @param filePath the file path for the queue
     * @return MessageQueue instance
     * @throws IOException if an I/O error occurs
     */
    public static MessageQueue createLargeMessageQueue(String filePath) throws IOException {
        return new PersistentMessageQueue(
                new QueueConfig.Builder()
                        .filePath(filePath)
                        .defaultBufferSize(16 * 1024 * 1024) // 16MB default buffer
                        .maxBufferSize(32 * 1024 * 1024) // 32MB max buffer
                        .maxFileSize(10L * 1024L * 1024L * 1024L) // 10GB max file size
                        .batchThreshold(16) // Smaller batch size for large messages
                        .build());
    }

    /**
     * Creates a queue optimized for low memory environments.
     * Features smaller buffers and batch sizes.
     * <p>
     * Configuration:
     * <ul>
     * <li>Default buffer size: 256KB</li>
     * <li>Max buffer size: 1MB</li>
     * <li>Batch threshold: 16</li>
     * </ul>
     *
     * @param filePath the file path for the queue
     * @return MessageQueue instance
     * @throws IOException if an I/O error occurs
     */
    public static MessageQueue createLowMemoryQueue(String filePath) throws IOException {
        return new PersistentMessageQueue(
                new QueueConfig.Builder()
                        .filePath(filePath)
                        .defaultBufferSize(256 * 1024) // 256KB default buffer
                        .maxBufferSize(1024 * 1024) // 1MB max buffer
                        .batchThreshold(16) // Small batch size
                        .maxFileSize(1024L * 1024L * 1024L) // 1GB max file size
                        .build());
    }

    /**
     * Creates a queue with debug logging enabled.
     * Features checksum verification and debug logging.
     * Note: Debug logging can impact performance.
     * <p>
     * Configuration:
     * <ul>
     * <li>Default buffer size: 1MB</li>
     * <li>Max buffer size: 2MB</li>
     * <li>Batch threshold: 32</li>
     * <li>Checksums: Enabled</li>
     * <li>Debug logging: Enabled</li>
     * </ul>
     *
     * @param filePath the file path for the queue
     * @return MessageQueue instance
     * @throws IOException if an I/O error occurs
     */
    public static MessageQueue createDebugQueue(String filePath) throws IOException {
        return new PersistentMessageQueue(
                new QueueConfig.Builder()
                        .filePath(filePath)
                        .debugEnabled(true)
                        .checksumEnabled(true)
                        .defaultBufferSize(1024 * 1024) // 1MB default buffer
                        .maxBufferSize(2 * 1024 * 1024) // 2MB max buffer
                        .batchThreshold(32)
                        .build());
    }

    /**
     * Creates a queue with a custom configuration.
     * Features configurable buffer sizes, batch threshold, and checksum
     * verification.
     * <p>
     * Configuration Defaults:
     * <ul>
     * <li>Default buffer size: 1MB</li>
     * <li>Max buffer size: 8MB</li>
     * <li>Batch threshold: 64</li>
     * <li>Checksums: Enabled</li>
     * <li>Debug logging: Enabled</li>
     * </ul>
     */
    public static class CustomQueueBuilder {
        private final QueueConfig.Builder builder;

        /**
         * Creates a new CustomQueueBuilder.
         */
        public CustomQueueBuilder() {
            this.builder = new QueueConfig.Builder();
        }

        /**
         * Sets the file path for the queue.
         *
         * @param filePath the file path for the queue
         * @return this
         */
        public CustomQueueBuilder withFilePath(String filePath) {
            builder.filePath(filePath);
            return this;
        }

        /**
         * Enables or disables debug mode.
         *
         * @param enabled true to enable debug logging
         * @return this
         */
        public CustomQueueBuilder withDebugEnabled(boolean enabled) {
            builder.debugEnabled(enabled);
            return this;
        }

        /**
         * Enables or disables checksums.
         *
         * @param enabled true to enable checksum verification
         * @return this
         */
        public CustomQueueBuilder withChecksumEnabled(boolean enabled) {
            builder.checksumEnabled(enabled);
            return this;
        }

        /**
         * Sets the maximum file size for the queue.
         *
         * @param maxFileSize the maximum file size
         * @return this
         */
        public CustomQueueBuilder withMaxFileSize(long maxFileSize) {
            builder.maxFileSize(maxFileSize);
            return this;
        }

        /**
         * Sets the initial file size for the queue.
         *
         * @param initialFileSize the initial file size
         * @return this
         */
        public CustomQueueBuilder withDefaultBufferSize(int initialFileSize) {
            builder.defaultBufferSize(initialFileSize);
            return this;
        }

        /**
         * Sets the maximum buffer size for the queue.
         *
         * @param size the maximum buffer size
         * @return this
         */
        public CustomQueueBuilder withMaxBufferSize(int size) {
            builder.maxBufferSize(size);
            return this;
        }

        /**
         * Sets the batch threshold for the queue.
         *
         * @param threshold the batch threshold
         * @return this
         */
        public CustomQueueBuilder withBatchThreshold(int threshold) {
            builder.batchThreshold(threshold);
            return this;
        }

        /**
         * Builds the MessageQueue instance.
         *
         * @return MessageQueue instance
         * @throws IOException if an I/O error occurs
         */
        public MessageQueue build() throws IOException {
            return new PersistentMessageQueue(builder.build());
        }
    }

    /**
     * Predefined queue configurations
     */
    public enum QueuePreset {
        /**
         * Configures the queue with high throughput settings.
         */
        HIGH_THROUGHPUT {
            /**
             * Configures the queue with high throughput settings.
             */
            @Override
            void configure(QueueConfig.Builder builder) {
                builder.defaultBufferSize(4 * 1024 * 1024)
                        .maxBufferSize(16 * 1024 * 1024)
                        .batchThreshold(256)
                        .checksumEnabled(false);
            }
        },
        /**
         * Configures the queue with durability settings.
         */
        DURABLE {
            /**
             * Configures the queue with durable settings.
             */
            @Override
            void configure(QueueConfig.Builder builder) {
                builder.defaultBufferSize(1024 * 1024)
                        .maxBufferSize(4 * 1024 * 1024)
                        .batchThreshold(32)
                        .checksumEnabled(true)
                        .debugEnabled(true);
            }
        },
        /**
         * Configures the queue with large message settings.
         */
        LOW_MEMORY {
            /**
             * Configures the queue with low memory settings.
             */
            @Override
            void configure(QueueConfig.Builder builder) {
                builder.defaultBufferSize(256 * 1024)
                        .maxBufferSize(1024 * 1024)
                        .batchThreshold(16);
            }
        };

        abstract void configure(QueueConfig.Builder builder);

        /**
         * Creates a new MessageQueue instance with the specified configuration.
         *
         * @param filePath the file path for the queue
         * @return MessageQueue instance
         * @throws IOException if an I/O error occurs
         */
        public MessageQueue createQueue(String filePath) throws IOException {
            QueueConfig.Builder builder = new QueueConfig.Builder().filePath(filePath);
            configure(builder);
            return new PersistentMessageQueue(builder.build());
        }
    }
}