package io.github.elimelt.pmqueue.consumer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Logger;

import io.github.elimelt.pmqueue.MessageQueue;
import io.github.elimelt.pmqueue.message.Message;

/**
 * Default implementation of MessageConsumer. This class consumes messages from
 * a
 * MessageQueue and processes them with a retry mechanism.
 * This class is thread-safe.
 *
 * Example usage:
 *
 * <pre>{@code
 * MessageQueue queue = new PersistentMessageQueue(
 *         new QueueConfig.Builder()
 *                 .filePath(filePath)
 *                 .build());
 * MessageConsumer consumer = new DefaultMessageConsumer(
 *         queue,
 *         message -> System.out.println(new String(message.getData())),
 *         3,
 *         1000);
 * consumer.start();
 * }</pre>
 */
public class DefaultMessageConsumer implements MessageConsumer {
    private static final Logger logger = Logger.getLogger(DefaultMessageConsumer.class.getName());

    private final MessageQueue queue;
    private final Consumer<Message> messageHandler;
    private final int maxRetries;
    private final long retryDelayMs;
    private final AtomicBoolean running;
    private Thread consumerThread;

    /**
     * Creates a new DefaultMessageConsumer.
     *
     * @param queue          the MessageQueue to consume messages from
     * @param messageHandler the message handler
     * @param maxRetries     the maximum number of retries
     * @param retryDelayMs   the delay between retries
     */
    public DefaultMessageConsumer(MessageQueue queue,
            Consumer<Message> messageHandler,
            int maxRetries,
            long retryDelayMs) {

        if (queue == null) {
            throw new IllegalArgumentException("queue cannot be null");
        }

        if (messageHandler == null) {
            throw new IllegalArgumentException("messageHandler cannot be null");
        }

        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0");
        }

        if (retryDelayMs < 0) {
            throw new IllegalArgumentException("retryDelayMs must be >= 0");
        }

        this.queue = queue;
        this.messageHandler = messageHandler;
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;
        this.running = new AtomicBoolean(false);
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            consumerThread = new Thread(this::consumeMessages);
            consumerThread.start();
        }
    }

    @Override
    public void stop() {
        running.set(false);
        if (consumerThread != null) {
            consumerThread.interrupt();
        }
    }

    private void consumeMessages() {
        while (running.get()) {
            try {
                Message message = queue.poll();
                if (message != null) {
                    processWithRetry(message);
                } else {
                    Thread.sleep(100); // Prevent tight loop
                }
            } catch (Exception e) {
                logger.warning("Error polling message: " + e.getMessage());
            }
        }
    }

    private void processWithRetry(Message message) {
        int attempts = 0;
        while (attempts < maxRetries) {
            try {
                messageHandler.accept(message);
                return;
            } catch (Exception e) {
                attempts++;
                if (attempts >= maxRetries) {
                    logger.severe("Failed to process message after " + maxRetries + " attempts");
                    break;
                }
                try {
                    Thread.sleep(retryDelayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * Returns true if the consumer is running.
     *
     * @return true if the consumer is running
     */
    public boolean getRunning() {
        return running.get();
    }

    /**
     * Returns the consumer thread.
     *
     * @return the consumer thread
     */
    public Thread getConsumerThread() {
        return consumerThread;
    }
}
