package io.github.elimelt.pmqueue;

import org.junit.jupiter.api.*;

import io.github.elimelt.pmqueue.message.Message;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

class QueueFactoryTest {
  private static final String TEST_DIR = "tmp/test_queues/";
  private Path testDir;

  @BeforeAll
  static void setUpTestDirectory() throws IOException {
    Files.createDirectories(Path.of(TEST_DIR));
  }

  @BeforeEach
  void setUp() throws IOException {
    // Create a unique test directory for each test
    testDir = Files.createDirectory(Path.of(TEST_DIR, UUID.randomUUID().toString()));
  }

  @AfterEach
  void tearDown() throws IOException {
    // Clean up test files
    deleteDirectory(testDir.toFile());
  }

  @AfterAll
  static void cleanUp() throws IOException {
    // Clean up main test directory
    deleteDirectory(new File(TEST_DIR));
  }

  private static void deleteDirectory(File directory) throws IOException {
    File[] files = directory.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          deleteDirectory(file);
        } else {
          Files.deleteIfExists(file.toPath());
        }
      }
    }
    Files.deleteIfExists(directory.toPath());
  }

  private String getTestFilePath(String prefix) {
    return testDir.resolve(prefix + ".dat").toString();
  }

  @Test
  @DisplayName("Default queue creation should use default configuration")
  void defaultQueueCreation() throws Exception {
    String filePath = getTestFilePath("default");
    MessageQueue queue = QueueFactory.createQueue(filePath);
    assertNotNull(queue);
    assertTrue(new File(filePath).exists());
    queue.close();
  }

  @Test
  @DisplayName("High throughput queue should be properly configured")
  void highThroughputQueueCreation() throws Exception {
    String filePath = getTestFilePath("high-throughput");
    MessageQueue queue = QueueFactory.createHighThroughputQueue(filePath);
    assertNotNull(queue);
    assertTrue(new File(filePath).exists());

    // Test high throughput characteristics by measuring operation speed
    long startTime = System.nanoTime();
    for (int i = 0; i < 1000; i++) {
      queue.offer(new Message(new byte[1024], 1));
    }
    long endTime = System.nanoTime();

    queue.close();

    // High throughput queue should be fast for batch operations
    assertTrue((endTime - startTime) / 1_000_000 < 1000); // Should complete in less than 1 second
  }

  @Test
  @DisplayName("Durable queue should maintain data integrity")
  void durableQueueCreation() throws Exception {
    String filePath = getTestFilePath("durable");
    MessageQueue queue = QueueFactory.createDurableQueue(filePath);
    assertNotNull(queue);
    assertTrue(new File(filePath).exists());

    // Test data durability
    byte[] testData = "test data".getBytes();
    Message original = new Message(testData, 1);
    queue.offer(original);
    queue.close();

    // Reopen queue and verify data
    MessageQueue reopenedQueue = QueueFactory.createDurableQueue(filePath);
    Message retrieved = reopenedQueue.poll();
    reopenedQueue.close();

    assertNotNull(retrieved);
    assertArrayEquals(original.getData(), retrieved.getData());
    assertEquals(original.getMessageType(), retrieved.getMessageType());
  }

  @Test
  @DisplayName("Large message queue should handle large messages")
  void largeMessageQueueCreation() throws Exception {
    String filePath = getTestFilePath("large-message");
    MessageQueue queue = QueueFactory.createLargeMessageQueue(filePath);
    assertNotNull(queue);
    assertTrue(new File(filePath).exists());

    // Test with a large message
    byte[] largeData = new byte[15 * 1024 * 1024]; // 15MB
    Message largeMessage = new Message(largeData, 1);
    assertTrue(queue.offer(largeMessage));

    Message retrieved = queue.poll();
    assertNotNull(retrieved);
    assertEquals(largeData.length, retrieved.getData().length);

    queue.close();
  }

  @Test
  @DisplayName("Low memory queue should use minimal resources")
  void lowMemoryQueueCreation() throws Exception {
    String filePath = getTestFilePath("low-memory");
    MessageQueue queue = QueueFactory.createLowMemoryQueue(filePath);
    assertNotNull(queue);
    assertTrue(new File(filePath).exists());

    // Test with small messages to verify queue operation
    for (int i = 0; i < 100; i++) {
      assertTrue(queue.offer(new Message(new byte[100], i)));
    }

    queue.close();
  }

  @Test
  @DisplayName("Debug queue should be created with debug settings")
  void debugQueueCreation() throws Exception {
    String filePath = getTestFilePath("debug");
    MessageQueue queue = QueueFactory.createDebugQueue(filePath);
    assertNotNull(queue);
    assertTrue(new File(filePath).exists());
    queue.close();
  }

  @Test
  @DisplayName("Custom queue builder should create queue with specified settings")
  void customQueueBuilderCreation() throws Exception {
    String filePath = getTestFilePath("custom");
    MessageQueue queue = new QueueFactory.CustomQueueBuilder()
        .withFilePath(filePath)
        .withDefaultBufferSize(2 * 1024 * 1024)
        .withMaxBufferSize(4 * 1024 * 1024)
        .withBatchThreshold(64)
        .withChecksumEnabled(true)
        .withDebugEnabled(true)
        .build();

    assertNotNull(queue);
    assertTrue(new File(filePath).exists());
    queue.close();
  }

  @Test
  @DisplayName("Queue presets should create properly configured queues")
  void queuePresetCreation() throws Exception {
    // Test each preset
    for (QueueFactory.QueuePreset preset : QueueFactory.QueuePreset.values()) {
      String filePath = getTestFilePath("preset-" + preset.name().toLowerCase());
      MessageQueue queue = preset.createQueue(filePath);

      assertNotNull(queue);
      assertTrue(new File(filePath).exists());

      // Test basic operations
      assertTrue(queue.offer(new Message("test".getBytes(), 1)));
      assertNotNull(queue.poll());
      assertTrue(queue.isEmpty());

      queue.close();
    }
  }

  @Test
  @DisplayName("Factory should handle invalid file paths")
  void invalidFilePathHandling() {
    String invalidPath = "\0invalid/path/to/queue.dat";
    assertThrows(IOException.class, () -> QueueFactory.createQueue(invalidPath));
  }

  @Test
  @DisplayName("Factory should handle concurrent queue creation")
  void concurrentQueueCreation() {
    assertTimeoutPreemptively(java.time.Duration.ofSeconds(5), () -> {
      var threads = new Thread[10];
      for (int i = 0; i < threads.length; i++) {
        final int index = i;
        threads[i] = new Thread(() -> {
          try {
            String filePath = getTestFilePath("concurrent-" + index);
            MessageQueue queue = QueueFactory.createQueue(filePath);
            queue.close();
          } catch (IOException e) {
            throw new RuntimeException(e);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
        threads[i].start();
      }

      for (Thread thread : threads) {
        thread.join();
      }
    });
  }
}