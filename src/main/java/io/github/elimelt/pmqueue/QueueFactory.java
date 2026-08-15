package io.github.elimelt.pmqueue;

import java.io.IOException;

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
 * QueueConfig config = new QueueConfig.Builder()
 *         .filePath(filePath)
 *         .debugEnabled(true)
 *         .checksumEnabled(true)
 *         .maxFileSize(1024 * 1024 * 1024)
 *         .defaultBufferSize(1024 * 1024)
 *         .maxBufferSize(8 * 1024 * 1024)
 *         .batchThreshold(64)
 *         .build();
 * MessageQueue queue = new PersistentMessageQueue(config);
 * }</pre>
 */
public class QueueFactory {
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
        return QueuePreset.HIGH_THROUGHPUT.createQueue(filePath);
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
        return QueuePreset.DURABLE.createQueue(filePath);
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
        return QueuePreset.LARGE_MESSAGE.createQueue(filePath);
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
        return QueuePreset.LOW_MEMORY.createQueue(filePath);
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
        return QueuePreset.DEBUG.createQueue(filePath);
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
         * Configures the queue with low memory settings.
         */
        LOW_MEMORY {
            /**
             * Configures the queue with low memory settings.
             */
            @Override
            void configure(QueueConfig.Builder builder) {
                builder.defaultBufferSize(256 * 1024)
                        .maxBufferSize(1024 * 1024)
                        .batchThreshold(16)
                        .maxFileSize(1024L * 1024L * 1024L);
            }
        },
        /**
         * Configures the queue with large message settings.
         */
        LARGE_MESSAGE {
            /**
             * Configures the queue with large message settings.
             */
            @Override
            void configure(QueueConfig.Builder builder) {
                builder.defaultBufferSize(16 * 1024 * 1024)
                        .maxBufferSize(32 * 1024 * 1024)
                        .maxFileSize(10L * 1024L * 1024L * 1024L)
                        .batchThreshold(16);
            }
        },
        /**
         * Configures the queue with debug settings.
         */
        DEBUG {
            /**
             * Configures the queue with debug settings.
             */
            @Override
            void configure(QueueConfig.Builder builder) {
                builder.debugEnabled(true)
                        .checksumEnabled(true)
                        .defaultBufferSize(1024 * 1024)
                        .maxBufferSize(2 * 1024 * 1024)
                        .batchThreshold(32);
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