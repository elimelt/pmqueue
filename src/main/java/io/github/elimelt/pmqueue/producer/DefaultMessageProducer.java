package io.github.elimelt.pmqueue.producer;

import java.io.IOException;

import io.github.elimelt.pmqueue.MessageQueue;
import io.github.elimelt.pmqueue.message.Message;

/**
 * Default implementation of MessageProducer. This class sends messages to a
 * MessageQueue.
 * This class is thread-safe.
 *
 * Example usage:
 *
 * <pre>{@code
 * MessageQueue queue = new PersistentMessageQueue(
 *         new QueueConfig.Builder()
 *                 .filePath(filePath)
 *                 .build());
 * MessageProducer producer = new DefaultMessageProducer(queue);
 * producer.send("Hello, World!".getBytes(), 1);
 * }</pre>
 */
public class DefaultMessageProducer implements MessageProducer {
    private final MessageQueue queue;

    /**
     * Creates a new DefaultMessageProducer.
     *
     * @param queue the MessageQueue to send messages to
     */
    public DefaultMessageProducer(MessageQueue queue) {
        if (queue == null) {
            throw new IllegalArgumentException("queue cannot be null");
        }
        this.queue = queue;
    }

    @Override
    public void send(byte[] data, int messageType) throws IOException {
        queue.offer(new Message(data, messageType));
    }

    @Override
    public void close() throws Exception {
        if (queue instanceof AutoCloseable) {
            ((AutoCloseable) queue).close();
        }
    }
}