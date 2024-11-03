package io.github.elimelt.pmqueue;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.util.*;

class PersistentMessageQueueTest {

  private static final String BASE_TEST_DIR = "tmp/test_queues/";
  private PersistentMessageQueue queue;
  private File testFile;
  private final Random random = new Random();

  @BeforeAll
  static void setUpTestDirectory() {
    new File(BASE_TEST_DIR).mkdirs();
  }

  @BeforeEach
  void setUp() throws IOException {
    // create unique file for each test
    testFile = new File(BASE_TEST_DIR + UUID.randomUUID() + ".dat");
    queue = new PersistentMessageQueue(testFile.getPath());
  }

  @AfterEach
  void tearDown() throws IOException {
    try {
      if (queue != null) {
        queue.close();
      }
      if (testFile != null && testFile.exists()) {
        testFile.delete();
      }
    } catch (IOException ignore) {
    }
  }

  @AfterAll
  static void cleanUpTestDirectory() {
    deleteDirectory(new File(BASE_TEST_DIR));
  }

  private static void deleteDirectory(File directory) {
    File[] files = directory.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          deleteDirectory(file);
        } else {
          file.delete();
        }
      }
    }
    directory.delete();
  }

  @Test
  void queueShouldBeEmptyWhenCreated() {
    assertTrue(queue.isEmpty(), "Newly created queue should be empty");
  }

  @Test
  void basicOfferAndPollShouldWork() throws IOException {
    Message message = new Message("test".getBytes(), 1);
    assertTrue(queue.offer(message));
    assertFalse(queue.isEmpty());

    Message retrieved = queue.poll();
    assertNotNull(retrieved);
    assertArrayEquals(message.getData(), retrieved.getData());
    assertEquals(message.getMessageType(), retrieved.getMessageType());
    assertTrue(queue.isEmpty());
  }

  @Test
  void shouldHandleNullMessageOffer() {
    assertThrows(NullPointerException.class, () -> queue.offer(null));
  }

  @Test
  void shouldHandleCorruptedFileHeader() throws IOException {
    queue.close();

    // corrupt header
    try (RandomAccessFile file = new RandomAccessFile(testFile, "rw")) {
      file.seek(0);
      file.writeLong(-1L); // invalid front offset
    }

    assertThrows(IOException.class, () -> new PersistentMessageQueue(testFile.getPath()));
  }

  @Test
  void fuzzTestRandomMessageSizes() throws IOException {
    List<Message> messages = new ArrayList<>();

    // messages with random sizes
    for (int i = 0; i < 100; i++) { //
      int size = random.nextInt(1024);
      byte[] data = new byte[size];
      random.nextBytes(data);
      Message msg = new Message(data, i);
      messages.add(msg);
      assertTrue(queue.offer(msg));
    }

    // verify messages
    for (Message original : messages) {
      Message retrieved = queue.poll();
      assertNotNull(retrieved);
      assertArrayEquals(original.getData(), retrieved.getData());
      assertEquals(original.getMessageType(), retrieved.getMessageType());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = { 0, 1, 100, 1024 })
  void fuzzTestSpecificMessageSizes(int size) throws IOException {
    byte[] data = new byte[size];
    random.nextBytes(data);
    Message msg = new Message(data, 1);

    assertTrue(queue.offer(msg));
    Message retrieved = queue.poll();

    assertNotNull(retrieved);
    assertArrayEquals(msg.getData(), retrieved.getData());
  }

  @Test
  void fuzzTestRandomBinaryData() throws IOException {
    List<Message> messages = new ArrayList<>();

    // write messages
    for (int i = 0; i < 100; i++) {
      byte[] data = new byte[random.nextInt(100)];
      random.nextBytes(data);
      // include random nulls and control characters
      for (int j = 0; j < data.length; j++) {
        if (random.nextInt(10) == 0) {
          data[j] = 0;
        }
      }
      Message msg = new Message(data, random.nextInt());
      messages.add(msg);
      assertTrue(queue.offer(msg));
    }

    // close and reopen queue
    queue.close();
    queue = new PersistentMessageQueue(testFile.getPath());

    // verify messages
    for (Message original : messages) {
      Message retrieved = queue.poll();
      assertNotNull(retrieved);
      assertArrayEquals(original.getData(), retrieved.getData());
      assertEquals(original.getMessageType(), retrieved.getMessageType());
    }
  }

  @Test
  void stressTestRapidOperations() throws IOException {
    for (int cycle = 0; cycle < 10; cycle++) {
      List<Message> messages = new ArrayList<>();

      // write messages
      for (int i = 0; i < 100; i++) {
        Message msg = new Message(("stress" + i).getBytes(), i);
        messages.add(msg);
        assertTrue(queue.offer(msg));
      }

      // verify messages
      for (Message original : messages) {
        Message retrieved = queue.poll();
        assertNotNull(retrieved);
        assertEquals(original.getMessageType(), retrieved.getMessageType());
        assertArrayEquals(original.getData(), retrieved.getData());
      }
    }
  }

  @Test
  void shouldHandleMaxMessageSize() throws IOException {
    byte[] largeData = new byte[1024 * 1024]; // 1MB
    Message msg = new Message(largeData, 1);
    assertTrue(queue.offer(msg));

    Message retrieved = queue.poll();
    assertNotNull(retrieved);
    assertArrayEquals(msg.getData(), retrieved.getData());
  }

  @Test
  void shouldHandleRapidOpenClose() throws IOException {
    List<Message> messages = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      queue.close();
      queue = new PersistentMessageQueue(testFile.getPath());
      Message msg = new Message(("cycle" + i).getBytes(), i);
      messages.add(msg);
      queue.offer(msg);
    }

    // verify messages
    queue.close();
    queue = new PersistentMessageQueue(testFile.getPath());
    for (Message original : messages) {
      Message retrieved = queue.poll();
      assertNotNull(retrieved);
      assertArrayEquals(original.getData(), retrieved.getData());
      assertEquals(original.getMessageType(), retrieved.getMessageType());
    }
  }
}