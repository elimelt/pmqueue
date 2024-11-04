package io.github.elimelt.pmqueue;

import java.io.IOException;

import io.github.elimelt.pmqueue.message.Message;

/**
 * Interface for message queues.
 * Implementations of this interface are responsible for storing and retrieving
 * messages.
 * This interface is thread-safe.
 *
 * Example usage:
 *
 * <pre>{@code
 * MessageQueue queue = new PersistentMessageQueue(
 *         new QueueConfig.Builder()
 *                 .filePath(filePath)
 *                 .build());
 * queue.offer(new Message("Hello, World!".getBytes(), 1));
 * Message message = queue.poll();
 *
 * }</pre>
 */
public interface MessageQueue extends AutoCloseable {

    /**
     * Stores a message in the queue.
     *
     * @param message the message to store
     * @return true if the message was stored, false otherwise
     * @throws IOException if an I/O error occurs
     */
    boolean offer(Message message) throws IOException;

    /**
     * Retrieves a message from the queue.
     *
     * @return the message retrieved from the queue
     * @throws IOException if an I/O error occurs
     */
    Message poll() throws IOException;

    /**
     * Checks if the queue is empty.
     *
     * @return true if the queue is empty, false otherwise
     */
    boolean isEmpty();
}