package io.github.elimelt.pmqueue.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.github.elimelt.pmqueue.message.Message;
import io.github.elimelt.pmqueue.message.MessageSerializer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.zip.CRC32;

/**
 * Characterization tests that pin the exact on-disk queue file layout
 * produced by {@link PersistentMessageQueue}, as documented in its class
 * Javadoc and file-format comment:
 *
 * <pre>
 * Queue header (24 bytes), all fields big-endian (java.nio.ByteBuffer's default order):
 *   offset 0  (8 bytes): front offset (long)
 *   offset 8  (8 bytes): rear offset (long)
 *   offset 16 (8 bytes): reserved
 *
 * Each message block:
 *   offset +0 (4 bytes): message size (int, big-endian)
 *   offset +4 (4 bytes): CRC32 checksum of the serialized message (int, big-endian)
 *   offset +8 (n bytes): serialized message (see MessageSerializer / WireFormatTest)
 * </pre>
 *
 * <p>
 * These tests write a real queue file with {@link PersistentMessageQueue},
 * close it, then read the raw bytes back with {@link RandomAccessFile} to
 * assert the header/block structure exactly. Later refactors of the queue
 * internals must keep producing byte-identical files.
 */
class FileFormatTest {

  private static final int QUEUE_HEADER_SIZE = 24;
  private static final int BLOCK_HEADER_SIZE = 8;

  @TempDir
  Path tempDir;

  @Test
  @DisplayName("Constants documented in the class Javadoc have not drifted")
  void headerConstantsAreUnchanged() {
    assertEquals(QUEUE_HEADER_SIZE, PersistentMessageQueue.QUEUE_HEADER_SIZE);
    assertEquals(BLOCK_HEADER_SIZE, PersistentMessageQueue.BLOCK_HEADER_SIZE);
  }

  @Test
  @DisplayName("A freshly created, empty queue file is exactly 24 bytes: front=24, rear=24, reserved=0")
  void newEmptyQueueFileHeaderLayout() throws IOException {
    File file = tempDir.resolve("empty.queue").toFile();

    PersistentMessageQueue queue = new PersistentMessageQueue(file.getPath());
    queue.close();

    assertEquals(QUEUE_HEADER_SIZE, file.length(),
        "A brand-new, empty queue file must be exactly QUEUE_HEADER_SIZE bytes");

    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
      raf.seek(0);
      long front = raf.readLong();
      long rear = raf.readLong();
      long reserved = raf.readLong();

      assertEquals(QUEUE_HEADER_SIZE, front, "Front offset must start at the header size");
      assertEquals(QUEUE_HEADER_SIZE, rear, "Rear offset must start at the header size");
      assertEquals(0L, reserved, "Reserved header bytes must be untouched/zero");
    }
  }

  @Test
  @DisplayName("Offering one message writes a block header (size, CRC32) then the serialized message right after the queue header")
  void singleMessageBlockLayout() throws IOException {
    File file = tempDir.resolve("single.queue").toFile();
    byte[] data = "hello".getBytes();
    int type = 5;
    Message message = new Message(data, type);

    // Capture the exact wire bytes PersistentMessageQueue will embed. The
    // message is immutable (fixed timestamp), so re-serializing it here
    // yields byte-identical output to what offer() serializes internally.
    byte[] expectedSerialized = MessageSerializer.serialize(message);
    CRC32 crc = new CRC32();
    crc.update(expectedSerialized);
    int expectedChecksum = (int) crc.getValue();

    PersistentMessageQueue queue = new PersistentMessageQueue(file.getPath());
    queue.offer(message);
    queue.close();

    long expectedRear = QUEUE_HEADER_SIZE + BLOCK_HEADER_SIZE + expectedSerialized.length;

    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
      raf.seek(0);
      long front = raf.readLong();
      long rear = raf.readLong();
      long reserved = raf.readLong();

      assertEquals(QUEUE_HEADER_SIZE, front, "Front offset unchanged: nothing has been polled");
      assertEquals(expectedRear, rear, "Rear offset must advance by block header + serialized message size");
      assertEquals(0L, reserved);

      raf.seek(QUEUE_HEADER_SIZE);
      int messageSize = raf.readInt();
      int checksum = raf.readInt();

      assertEquals(expectedSerialized.length, messageSize,
          "Block header message-size field must equal the serialized message length");
      assertEquals(expectedChecksum, checksum,
          "Block header checksum field must equal CRC32 of the serialized message bytes");

      byte[] actualSerialized = new byte[expectedSerialized.length];
      raf.readFully(actualSerialized);
      assertArrayEquals(expectedSerialized, actualSerialized,
          "Bytes right after the block header must be exactly the serialized message");
    }

    // Note: PersistentMessageQueue.offer() pre-grows the file with headroom
    // (see the file.setLength() sizing in offer()) rather than truncating to
    // exactly the bytes written, so total file length is not pinned here -
    // only the header and block layout within it are.
    assertTrue(file.length() >= QUEUE_HEADER_SIZE + BLOCK_HEADER_SIZE + expectedSerialized.length,
        "File must be at least large enough to hold the header and the one block written");
  }

  @Test
  @DisplayName("After polling the only message, front offset catches up to rear offset; underlying block bytes are left in place")
  void frontOffsetAdvancesAfterPollWithoutErasingData() throws IOException {
    File file = tempDir.resolve("polled.queue").toFile();
    Message message = new Message("bye".getBytes(), 3);
    byte[] expectedSerialized = MessageSerializer.serialize(message);

    PersistentMessageQueue queue = new PersistentMessageQueue(file.getPath());
    queue.offer(message);
    queue.poll();
    queue.close();

    long expectedOffset = QUEUE_HEADER_SIZE + BLOCK_HEADER_SIZE + expectedSerialized.length;

    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
      raf.seek(0);
      long front = raf.readLong();
      long rear = raf.readLong();

      assertEquals(expectedOffset, front, "Front offset must catch up to rear after draining the queue");
      assertEquals(expectedOffset, rear);
      assertTrue(front == rear, "Queue file header must show an empty queue after poll");

      // Block bytes are not erased by poll(); they remain on disk past the
      // (now equal) front/rear offsets.
      raf.seek(QUEUE_HEADER_SIZE);
      int messageSize = raf.readInt();
      assertEquals(expectedSerialized.length, messageSize,
          "poll() must not erase the block header/data still physically on disk");
    }
  }
}
