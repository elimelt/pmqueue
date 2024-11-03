package io.github.elimelt.pmqueue;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PersistentMessageQueueTest {

    private static final String TEST_FILE = "test_queue.dat";
    private PersistentMessageQueue queue;

    @BeforeEach
    void setUp() throws IOException {
        queue = new PersistentMessageQueue(TEST_FILE);
    }

    @AfterEach
    void tearDown() throws IOException {
        queue.close();
        new File(TEST_FILE).delete();
    }

    @Test
    @Order(1)
    @DisplayName("Queue should be empty when created")
    void queueShouldBeEmptyWhenCreated() {
        assertTrue(queue.isEmpty(), "Newly created queue should be empty");
    }

    @Test
    @Order(2)
    @DisplayName("Basic offer and poll operations should work")
    void basicOfferAndPollShouldWork() throws IOException {
        Message message = new Message("test".getBytes(), 1);
        assertTrue(queue.offer(message), "Offer should succeed");
        assertFalse(queue.isEmpty(), "Queue should not be empty after offer");
        
        Message retrieved = queue.poll();
        assertNotNull(retrieved, "Poll should return message");
        assertArrayEquals(message.getData(), retrieved.getData(), "Message data should match");
        assertEquals(message.getMessageType(), retrieved.getMessageType(), "Message type should match");
        
        assertTrue(queue.isEmpty(), "Queue should be empty after poll");
    }

    @Test
    @Order(3)
    @DisplayName("Queue should persist messages across restarts")
    void queueShouldPersistMessages() throws IOException {
        Message message = new Message("persistent".getBytes(), 1);
        queue.offer(message);
        queue.close();

        // Reopen queue
        queue = new PersistentMessageQueue(TEST_FILE);
        Message retrieved = queue.poll();
        
        assertNotNull(retrieved, "Message should persist after restart");
        assertArrayEquals(message.getData(), retrieved.getData(), "Message data should persist correctly");
    }

    @Test
    @Order(4)
    @DisplayName("Queue should handle multiple messages")
    void queueShouldHandleMultipleMessages() throws IOException {
        int messageCount = 100;
        List<Message> messages = new ArrayList<>();
        
        // Offer messages
        for (int i = 0; i < messageCount; i++) {
            Message msg = new Message(("message" + i).getBytes(), i);
            messages.add(msg);
            assertTrue(queue.offer(msg), "Offer should succeed for message " + i);
        }
        
        // Poll and verify messages
        for (int i = 0; i < messageCount; i++) {
            Message original = messages.get(i);
            Message retrieved = queue.poll();

            assertNotNull(retrieved, "Should retrieve message " + i);
            assertArrayEquals(original.getData(), retrieved.getData(),
                "Data should match for message " + i);
            assertEquals(original.getMessageType(), retrieved.getMessageType(),
                "Type should match for message " + i);
        }

        assertTrue(queue.isEmpty(), "Queue should be empty after polling all messages");
    }

    @Test
    @Order(5)
    @DisplayName("Queue should handle concurrent operations")
    void queueShouldHandleConcurrentOperations() throws InterruptedException {
        int threadCount = 10;
        int messagesPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount * 2); // For both producers and consumers
        
        // Start producer threads
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < messagesPerThread; i++) {
                        Message msg = new Message(
                            ("thread" + threadId + "msg" + i).getBytes(),
                            threadId * 1000 + i
                        );
                        queue.offer(msg);
                    }
                } catch (IOException e) {
                    fail("Producer thread failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // Start consumer threads
        ConcurrentHashMap<String, Integer> messageCount = new ConcurrentHashMap<>();
        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    while (true) {
                        Message msg = queue.poll();
                        if (msg == null) {
                            // Check if all messages have been processed
                            if (messageCount.size() == threadCount * messagesPerThread) {
                                break;
                            }
                            Thread.sleep(10);
                            continue;
                        }
                        messageCount.put(new String(msg.getData()), msg.getMessageType());
                    }
                } catch (Exception e) {
                    fail("Consumer thread failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS), "Operations timed out");
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor shutdown timed out");

        assertEquals(threadCount * messagesPerThread, messageCount.size(),
            "Should process all messages exactly once");
    }

    @Test
    @Order(6)
    @DisplayName("Queue should handle large messages")
    void queueShouldHandleLargeMessages() throws IOException {
        byte[] largeData = new byte[1024 * 1024]; // 1MB
        ThreadLocalRandom.current().nextBytes(largeData);

        Message message = new Message(largeData, 1);
        assertTrue(queue.offer(message), "Should accept large message");

        Message retrieved = queue.poll();
        assertNotNull(retrieved, "Should retrieve large message");
        assertArrayEquals(largeData, retrieved.getData(), "Large message data should match");
    }

    @Test
    @Order(7)
    @DisplayName("Queue should maintain data integrity after compaction")
    void queueShouldMaintainIntegrityAfterCompaction() throws IOException {
        // Fill queue with messages
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            Message msg = new Message(("compaction test " + i).getBytes(), i);
            messages.add(msg);
            queue.offer(msg);
        }

        // Poll half the messages
        for (int i = 0; i < 500; i++) {
            assertNotNull(queue.poll(), "Should retrieve message during compaction test");
        }

        // Add more messages
        for (int i = 1000; i < 1500; i++) {
            Message msg = new Message(("compaction test " + i).getBytes(), i);
            messages.add(msg);
            queue.offer(msg);
        }

        // Verify remaining messages
        for (int i = 500; i < messages.size(); i++) {
            Message original = messages.get(i);
            Message retrieved = queue.poll();

            assertNotNull(retrieved, "Should retrieve message after compaction");
            assertArrayEquals(original.getData(), retrieved.getData(),
                "Message data should be preserved after compaction");
            assertEquals(original.getMessageType(), retrieved.getMessageType(),
                "Message type should be preserved after compaction");
        }
    }
}