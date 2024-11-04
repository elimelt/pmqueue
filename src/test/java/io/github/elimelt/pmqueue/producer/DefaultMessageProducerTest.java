package io.github.elimelt.pmqueue.producer;

import io.github.elimelt.pmqueue.MessageQueue;
import io.github.elimelt.pmqueue.message.Message;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class DefaultMessageProducerTest {
  private MessageQueue mockQueue;
  private DefaultMessageProducer producer;

  @BeforeEach
  void setUp() {
    mockQueue = mock(MessageQueue.class);
    producer = new DefaultMessageProducer(mockQueue);
  }

  @Test
  @DisplayName("Constructor should accept MessageQueue")
  void constructorShouldAcceptMessageQueue() {
    assertDoesNotThrow(() -> new DefaultMessageProducer(mockQueue));
  }

  @Test
  @DisplayName("Constructor should reject null queue")
  void constructorShouldRejectNullQueue() {
    assertThrows(IllegalArgumentException.class, () -> new DefaultMessageProducer(null));
  }

  @Test
  @DisplayName("Send should delegate to queue.offer")
  void sendShouldDelegateToQueueOffer() throws IOException {
    byte[] data = "test".getBytes();
    int messageType = 1;
    when(mockQueue.offer(any(Message.class))).thenReturn(true);

    producer.send(data, messageType);

    verify(mockQueue).offer(argThat(message -> java.util.Arrays.equals(data, message.getData()) &&
        message.getMessageType() == messageType));
  }

  @Test
  @DisplayName("Send should handle null data")
  void sendShouldHandleNullData() throws IOException {
    assertThrows(NullPointerException.class, () -> producer.send(null, 1));
    verify(mockQueue, never()).offer(any());
  }

  @ParameterizedTest
  @ValueSource(ints = { Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE })
  @DisplayName("Send should accept any message type")
  void sendShouldAcceptAnyMessageType(int messageType) throws IOException {
    byte[] data = "test".getBytes();
    when(mockQueue.offer(any(Message.class))).thenReturn(true);

    producer.send(data, messageType);

    verify(mockQueue).offer(argThat(message -> message.getMessageType() == messageType));
  }

  @Test
  @DisplayName("Send should handle queue.offer returning false")
  void sendShouldHandleOfferReturningFalse() throws IOException {
    when(mockQueue.offer(any(Message.class))).thenReturn(false);
    byte[] data = "test".getBytes();

    producer.send(data, 1);

    verify(mockQueue).offer(any(Message.class));
  }

  @Test
  @DisplayName("Send should propagate IOException from queue")
  void sendShouldPropagateIOException() throws IOException {
    when(mockQueue.offer(any(Message.class)))
        .thenThrow(new IOException("Test exception"));

    assertThrows(IOException.class, () -> producer.send("test".getBytes(), 1));
  }

  @Test
  @DisplayName("Close should handle AutoCloseable queue")
  void closeShouldHandleAutoCloseableQueue() throws Exception {
    MessageQueue autoCloseableQueue = mock(MessageQueue.class,
        withSettings().extraInterfaces(AutoCloseable.class));
    DefaultMessageProducer autoCloseableProducer = new DefaultMessageProducer(autoCloseableQueue);

    autoCloseableProducer.close();

    verify((AutoCloseable) autoCloseableQueue).close();
  }

  @Test
  @DisplayName("Close should handle non-AutoCloseable queue")
  void closeShouldHandleNonAutoCloseableQueue() {
    assertDoesNotThrow(() -> producer.close());
  }

  @Test
  @DisplayName("Send should handle messages of various sizes")
  void sendShouldHandleVariousMessageSizes() throws IOException {
    when(mockQueue.offer(any(Message.class))).thenReturn(true);

    // Test empty message
    producer.send(new byte[0], 1);
    verify(mockQueue).offer(argThat(msg -> msg.getData().length == 0));

    // Test small message
    producer.send(new byte[10], 2);
    verify(mockQueue).offer(argThat(msg -> msg.getData().length == 10));

    // Test large message
    producer.send(new byte[1024 * 1024], 3);
    verify(mockQueue).offer(argThat(msg -> msg.getData().length == 1024 * 1024));
  }

  @Test
  @DisplayName("Producer should handle concurrent sends")
  void shouldHandleConcurrentSends() throws InterruptedException, IOException {
    int threadCount = 10;
    int messagesPerThread = 100;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(threadCount);
    AtomicInteger successCount = new AtomicInteger(0);

    when(mockQueue.offer(any(Message.class))).thenReturn(true);

    // Create threads that will send messages concurrently
    for (int i = 0; i < threadCount; i++) {
      final int threadId = i;
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int j = 0; j < messagesPerThread; j++) {
            try {
              producer.send(
                  ("thread-" + threadId + "-msg-" + j).getBytes(),
                  j);
              successCount.incrementAndGet();
            } catch (IOException e) {
              // Count will not be incremented for failed sends
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          completionLatch.countDown();
        }
      });
    }

    // Start all threads simultaneously
    startLatch.countDown();

    // Wait for completion
    assertTrue(completionLatch.await(10, TimeUnit.SECONDS));
    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

    // Verify results
    assertEquals(threadCount * messagesPerThread, successCount.get());
    verify(mockQueue, times(threadCount * messagesPerThread)).offer(any(Message.class));
  }

  @Test
  @DisplayName("Producer should maintain thread safety during errors")
  void shouldMaintainThreadSafetyDuringErrors() throws InterruptedException, IOException {
    int threadCount = 10;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(threadCount);
    AtomicInteger errorCount = new AtomicInteger(0);

    when(mockQueue.offer(any(Message.class)))
        .thenThrow(new IOException("Simulated error"));

    // Create threads that will encounter errors
    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          try {
            producer.send("test".getBytes(), 1);
          } catch (IOException e) {
            errorCount.incrementAndGet();
          }
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          completionLatch.countDown();
        }
      });
    }

    // Start all threads simultaneously
    startLatch.countDown();

    // Wait for completion
    assertTrue(completionLatch.await(10, TimeUnit.SECONDS));
    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

    // Verify all threads encountered errors
    assertEquals(threadCount, errorCount.get());
  }
}