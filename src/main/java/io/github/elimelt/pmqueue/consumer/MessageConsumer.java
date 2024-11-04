package io.github.elimelt.pmqueue.consumer;

/**
 * Interface for consuming messages.
 * Implementations of this interface are responsible for consuming messages from
 * a
 * message queue.
 * This interface is thread-safe.
 *
 * Example usage:
 *
 * <pre>{@code
 * MessageQueue queue = new PersistentMessageQueue(
 *     new QueueConfig.Builder()
 *         .filePath(filePath)
 *         .build());
 * MessageConsumer consumer = new DefaultMessageConsumer(
 *     queue,
 *     message -> System.out.println(new String(message.getData())),
 *     3,
 *     1000);
 * consumer.start();
 * }</pre>
 */
public interface MessageConsumer extends AutoCloseable {

  /**
   * Starts consuming messages.
   */
  void start();

  /**
   * Stops consuming messages.
   */
  void stop();
}