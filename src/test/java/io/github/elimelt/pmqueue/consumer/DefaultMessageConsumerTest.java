package io.github.elimelt.pmqueue.consumer;

import io.github.elimelt.pmqueue.MessageQueue;
import io.github.elimelt.pmqueue.message.Message;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

class DefaultMessageConsumerTest {
    private MessageQueue mockQueue;
    private Consumer<Message> mockHandler;
    private DefaultMessageConsumer consumer;
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long DEFAULT_RETRY_DELAY = 100;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mockQueue = mock(MessageQueue.class);
        mockHandler = mock(Consumer.class);
        consumer = new DefaultMessageConsumer(
                mockQueue,
                mockHandler,
                DEFAULT_MAX_RETRIES,
                DEFAULT_RETRY_DELAY);
    }

    @AfterEach
    void tearDown() {
        consumer.stop();
    }

    @Test
    @DisplayName("Constructor should validate parameters")
    void constructorShouldValidateParameters() {
        assertThrows(IllegalArgumentException.class,
                () -> new DefaultMessageConsumer(null, mockHandler, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_DELAY));

        assertThrows(IllegalArgumentException.class,
                () -> new DefaultMessageConsumer(mockQueue, null, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_DELAY));
    }

    @ParameterizedTest
    @ValueSource(ints = { -1 })
    @DisplayName("Constructor should reject invalid retry counts")
    void constructorShouldRejectInvalidRetryCount(int retries) {
        assertThrows(IllegalArgumentException.class,
                () -> new DefaultMessageConsumer(mockQueue, mockHandler, retries, DEFAULT_RETRY_DELAY));
    }

    @ParameterizedTest
    @ValueSource(longs = { -1L })
    @DisplayName("Constructor should reject invalid retry delays")
    void constructorShouldRejectInvalidRetryDelay(long delay) {
        assertThrows(IllegalArgumentException.class,
                () -> new DefaultMessageConsumer(mockQueue, mockHandler, DEFAULT_MAX_RETRIES, delay));
    }

    @Test
    @DisplayName("Consumer should process messages successfully")
    void shouldProcessMessagesSuccessfully() throws Exception {
        Message message = new Message("test".getBytes(), 1);
        CountDownLatch processLatch = new CountDownLatch(1);

        when(mockQueue.poll())
                .thenReturn(message)
                .thenReturn(null);

        doAnswer(invocation -> {
            processLatch.countDown();
            return null;
        }).when(mockHandler).accept(message);

        consumer.start();
        assertTrue(processLatch.await(1, TimeUnit.SECONDS));

        verify(mockHandler).accept(message);
    }

    @Test
    @DisplayName("Consumer should retry failed messages")
    void shouldRetryFailedMessages() throws Exception {
        Message message = new Message("test".getBytes(), 1);
        AtomicInteger attempts = new AtomicInteger(0);
        CountDownLatch processLatch = new CountDownLatch(1);

        when(mockQueue.poll())
                .thenReturn(message)
                .thenReturn(null);

        doAnswer(invocation -> {
            if (attempts.incrementAndGet() < 3) {
                throw new RuntimeException("Simulated failure");
            }
            processLatch.countDown();
            return null;
        }).when(mockHandler).accept(message);

        consumer.start();
        assertTrue(processLatch.await(2, TimeUnit.SECONDS));

        verify(mockHandler, times(3)).accept(message);
    }

    @Test
    @DisplayName("Consumer should handle permanent failures")
    void shouldHandlePermanentFailures() throws Exception {
        Message message = new Message("test".getBytes(), 1);
        CountDownLatch failureLatch = new CountDownLatch(DEFAULT_MAX_RETRIES);

        when(mockQueue.poll())
                .thenReturn(message)
                .thenReturn(null);

        doAnswer(invocation -> {
            failureLatch.countDown();
            throw new RuntimeException("Permanent failure");
        }).when(mockHandler).accept(message);

        consumer.start();
        assertTrue(failureLatch.await(2, TimeUnit.SECONDS));

        verify(mockHandler, times(DEFAULT_MAX_RETRIES)).accept(message);
    }

    @Test
    @DisplayName("Consumer should stop when requested")
    void shouldStopWhenRequested() throws Exception {
        CountDownLatch pollLatch = new CountDownLatch(1);
        when(mockQueue.poll()).thenAnswer(invocation -> {
            pollLatch.countDown();
            return null;
        });

        consumer.start();
        assertTrue(pollLatch.await(1, TimeUnit.SECONDS));
        consumer.stop();

        Thread.sleep(200); // Give time for the thread to stop
        verify(mockQueue, atLeast(1)).poll();
    }

    @Test
    @DisplayName("Consumer should handle queue exceptions")
    void shouldHandleQueueExceptions() throws Exception {
        CountDownLatch exceptionLatch = new CountDownLatch(1);
        when(mockQueue.poll())
                .thenThrow(new IOException("Queue error"))
                .thenAnswer(invocation -> {
                    exceptionLatch.countDown();
                    return null;
                });

        consumer.start();
        assertTrue(exceptionLatch.await(1, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("Consumer should not start multiple times")
    void shouldNotStartMultipleTimes() throws Exception {
        consumer.start();
        Thread firstThread = consumer.getConsumerThread();

        // Try to start again
        consumer.start();
        assertEquals(firstThread, consumer.getConsumerThread());
    }

    @Test
    @DisplayName("Consumer should handle interrupted retry")
    void shouldHandleInterruptedRetry() throws Exception {
        Message message = new Message("test".getBytes(), 1);
        CountDownLatch interruptLatch = new CountDownLatch(1);

        when(mockQueue.poll())
                .thenReturn(message)
                .thenReturn(null);

        doAnswer(invocation -> {
            Thread.currentThread().interrupt();
            interruptLatch.countDown();
            throw new RuntimeException("Simulated failure");
        }).when(mockHandler).accept(message);

        consumer.start();
        assertTrue(interruptLatch.await(1, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("Consumer should process multiple messages")
    void shouldProcessMultipleMessages() throws Exception {
        Message message1 = new Message("test1".getBytes(), 1);
        Message message2 = new Message("test2".getBytes(), 2);
        CountDownLatch processLatch = new CountDownLatch(2);

        when(mockQueue.poll())
                .thenReturn(message1)
                .thenReturn(message2)
                .thenReturn(null);

        doAnswer(invocation -> {
            processLatch.countDown();
            return null;
        }).when(mockHandler).accept(any());

        consumer.start();
        assertTrue(processLatch.await(1, TimeUnit.SECONDS));

        verify(mockHandler).accept(message1);
        verify(mockHandler).accept(message2);
    }

    @Test
    @DisplayName("Consumer should handle close properly")
    void shouldHandleCloseProperly() throws Exception {
        CountDownLatch startLatch = new CountDownLatch(1);
        when(mockQueue.poll()).thenAnswer(invocation -> {
            startLatch.countDown();
            return null;
        });

        consumer.start();
        assertTrue(startLatch.await(1, TimeUnit.SECONDS));

        consumer.close();
        Thread.sleep(200); // Give time for the thread to stop
        assertFalse(consumer.getRunning());
    }

    @Test
    @DisplayName("Consumer should respect retry delay")
    void shouldRespectRetryDelay() throws Exception {
        Message message = new Message("test".getBytes(), 1);
        AtomicInteger attempts = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();
        CountDownLatch processLatch = new CountDownLatch(DEFAULT_MAX_RETRIES);

        when(mockQueue.poll())
                .thenReturn(message)
                .thenReturn(null);

        doAnswer(invocation -> {
            attempts.incrementAndGet();
            processLatch.countDown();
            throw new RuntimeException("Simulated failure");
        }).when(mockHandler).accept(message);

        consumer.start();
        assertTrue(processLatch.await(2, TimeUnit.SECONDS));
        long duration = System.currentTimeMillis() - startTime;

        assertTrue(duration >= DEFAULT_RETRY_DELAY * (DEFAULT_MAX_RETRIES - 1),
                "Retry delay should be respected");
    }
}