package io.github.elimelt.pmqueue.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.github.elimelt.pmqueue.QueueConfig;
import io.github.elimelt.pmqueue.message.Message;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Regression tests for per-instance configuration in
 * {@link PersistentMessageQueue}.
 *
 * <p>
 * Before this fix, the queue's configuration (checksum enablement, buffer
 * sizes, batch threshold, etc.) was stored in {@code static} fields, so
 * creating a second queue instance with different settings clobbered the
 * configuration of every other open instance. In particular, opening a
 * checksum-enabled queue while a checksum-disabled queue was already open
 * would flip the shared {@code shouldChecksum} flag to {@code true} for
 * both, while the checksum-disabled instance still had a {@code null}
 * {@code checksumCalculator}, causing a {@link NullPointerException} on the
 * next {@code offer}/{@code poll}.
 */
class MultiInstanceConfigTest {

  @TempDir
  Path tempDir;

  @Test
  @DisplayName("Concurrently open queues with different checksum settings offer/poll correctly")
  void queuesWithDifferentChecksumSettingsDoNotInterfere() throws IOException {
    QueueConfig noChecksumConfig = new QueueConfig.Builder()
        .filePath(tempDir.resolve("no-checksum.dat").toString())
        .checksumEnabled(false)
        .build();

    try (PersistentMessageQueue noChecksumQueue = new PersistentMessageQueue(noChecksumConfig)) {
      // Use the checksum-disabled queue before a second, differently
      // configured instance exists.
      Message first = new Message("no-checksum-before".getBytes(), 1);
      assertTrue(noChecksumQueue.offer(first));

      QueueConfig checksumConfig = new QueueConfig.Builder()
          .filePath(tempDir.resolve("checksum.dat").toString())
          .checksumEnabled(true)
          .build();

      try (PersistentMessageQueue checksumQueue = new PersistentMessageQueue(checksumConfig)) {
        // Opening the second (checksum-enabled) queue must not change the
        // behavior of the first (checksum-disabled) queue: this offer/poll
        // pair NPEs before the per-instance-config fix.
        Message afterOpen = new Message("no-checksum-after".getBytes(), 2);
        assertTrue(noChecksumQueue.offer(afterOpen));

        Message polledFirst = noChecksumQueue.poll();
        assertNotNull(polledFirst);
        assertArrayEquals(first.getData(), polledFirst.getData());
        assertEquals(first.getMessageType(), polledFirst.getMessageType());

        Message polledAfterOpen = noChecksumQueue.poll();
        assertNotNull(polledAfterOpen);
        assertArrayEquals(afterOpen.getData(), polledAfterOpen.getData());
        assertEquals(afterOpen.getMessageType(), polledAfterOpen.getMessageType());
        assertTrue(noChecksumQueue.isEmpty());

        // The checksum-enabled queue must independently validate its own
        // messages via CRC32.
        Message checksummed = new Message("checksummed-payload".getBytes(), 3);
        assertTrue(checksumQueue.offer(checksummed));

        Message polledChecksummed = checksumQueue.poll();
        assertNotNull(polledChecksummed);
        assertArrayEquals(checksummed.getData(), polledChecksummed.getData());
        assertEquals(checksummed.getMessageType(), polledChecksummed.getMessageType());
        assertTrue(checksumQueue.isEmpty());
      }
    }
  }

  @Test
  @DisplayName("Concurrently open queues with different batch thresholds and buffer sizes keep independent config")
  void queuesWithDifferentBatchAndBufferConfigDoNotInterfere() throws IOException {
    QueueConfig smallBatchConfig = new QueueConfig.Builder()
        .filePath(tempDir.resolve("small-batch.dat").toString())
        .batchThreshold(1)
        .build();

    QueueConfig largeBatchConfig = new QueueConfig.Builder()
        .filePath(tempDir.resolve("large-batch.dat").toString())
        .batchThreshold(50)
        .build();

    try (PersistentMessageQueue smallBatchQueue = new PersistentMessageQueue(smallBatchConfig);
        PersistentMessageQueue largeBatchQueue = new PersistentMessageQueue(largeBatchConfig)) {

      // Interleave operations across both instances; if batchThreshold were
      // still shared static state, opening largeBatchQueue after
      // smallBatchQueue would overwrite smallBatchQueue's threshold.
      for (int i = 0; i < 5; i++) {
        assertTrue(smallBatchQueue.offer(new Message(("small-" + i).getBytes(), i)));
        assertTrue(largeBatchQueue.offer(new Message(("large-" + i).getBytes(), i)));
      }

      for (int i = 0; i < 5; i++) {
        Message smallMsg = smallBatchQueue.poll();
        assertNotNull(smallMsg);
        assertArrayEquals(("small-" + i).getBytes(), smallMsg.getData());

        Message largeMsg = largeBatchQueue.poll();
        assertNotNull(largeMsg);
        assertArrayEquals(("large-" + i).getBytes(), largeMsg.getData());
      }

      assertTrue(smallBatchQueue.isEmpty());
      assertTrue(largeBatchQueue.isEmpty());
      assertFalse(smallBatchQueue == largeBatchQueue);
    }
  }
}
