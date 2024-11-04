package io.github.elimelt.pmqueue.producer;

import java.io.IOException;

/**
 * Interface for sending messages.
 * Implementations of this interface are responsible for sending messages to a
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
 * MessageProducer producer = new DefaultMessageProducer(queue);
 * producer.send("Hello, World!".getBytes(), 1);
 * }</pre>
 */
public interface MessageProducer extends AutoCloseable {

  /**
   * Sends a message to a message queue.
   *
   * @param data        the message data
   * @param messageType the message type
   * @throws IOException if an I/O error occurs
   */
  void send(byte[] data, int messageType) throws IOException;
}