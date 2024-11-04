package io.github.elimelt.pmqueue;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

import io.github.elimelt.pmqueue.core.PersistentMessageQueue;

class QueueConfigTest {

  @Test
  @DisplayName("Builder should create config with default values")
  void builderShouldCreateConfigWithDefaults() {
    QueueConfig config = new QueueConfig.Builder().build();

    assertEquals("queue.dat", config.getFilePath());
    assertFalse(config.isDebugEnabled());
    assertTrue(config.isChecksumEnabled());
    assertEquals(1024L * 1024L * 1024L, config.getMaxFileSize());
    assertEquals(PersistentMessageQueue.QUEUE_HEADER_SIZE, config.getInitialFileSize());
    assertEquals(1024 * 1024, config.getDefaultBufferSize());
    assertEquals(8 * 1024 * 1024, config.getMaxBufferSize());
    assertEquals(64, config.getBatchThreshold());
  }

  @Test
  @DisplayName("Builder should allow customization of all values")
  void builderShouldAllowCustomization() {
    QueueConfig config = new QueueConfig.Builder()
        .filePath("custom.dat")
        .debugEnabled(true)
        .checksumEnabled(false)
        .maxFileSize(2048L)
        .initialFileSize(PersistentMessageQueue.QUEUE_HEADER_SIZE + 100)
        .defaultBufferSize(2048)
        .maxBufferSize(4096)
        .batchThreshold(128)
        .build();

    assertEquals("custom.dat", config.getFilePath());
    assertTrue(config.isDebugEnabled());
    assertFalse(config.isChecksumEnabled());
    assertEquals(2048L, config.getMaxFileSize());
    assertEquals(PersistentMessageQueue.QUEUE_HEADER_SIZE + 100, config.getInitialFileSize());
    assertEquals(2048, config.getDefaultBufferSize());
    assertEquals(4096, config.getMaxBufferSize());
    assertEquals(128, config.getBatchThreshold());
  }

  @Test
  @DisplayName("Builder should validate buffer size relationship")
  void builderShouldValidateBufferSizes() {
    QueueConfig.Builder builder = new QueueConfig.Builder()
        .defaultBufferSize(4096)
        .maxBufferSize(2048);

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        builder::build);
    assertTrue(exception.getMessage().contains("maxBufferSize must be >= defaultBufferSize"));
  }

  @Test
  @DisplayName("Builder should validate initial file size")
  void builderShouldValidateInitialFileSize() {
    QueueConfig.Builder builder = new QueueConfig.Builder()
        .initialFileSize(PersistentMessageQueue.QUEUE_HEADER_SIZE - 1);

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        builder::build);
    assertTrue(exception.getMessage().contains("initialFileSize must be >="));
  }

  @Test
  @DisplayName("Builder should validate batch threshold")
  void builderShouldValidateBatchThreshold() {
    QueueConfig.Builder builder = new QueueConfig.Builder()
        .batchThreshold(0);

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        builder::build);
    assertTrue(exception.getMessage().contains("batchThreshold must be > 0"));
  }

  @ParameterizedTest
  @ValueSource(ints = { 1, 10, 100, 1000 })
  @DisplayName("Builder should accept valid batch thresholds")
  void builderShouldAcceptValidBatchThresholds(int threshold) {
    QueueConfig config = new QueueConfig.Builder()
        .batchThreshold(threshold)
        .build();

    assertEquals(threshold, config.getBatchThreshold());
  }

  @Test
  @DisplayName("Config should be immutable")
  void configShouldBeImmutable() {
    QueueConfig.Builder builder = new QueueConfig.Builder()
        .filePath("test.dat")
        .defaultBufferSize(2048)
        .maxBufferSize(4096);

    QueueConfig config = builder.build();

    // Modify builder after creating config
    builder.filePath("modified.dat")
        .defaultBufferSize(8192)
        .maxBufferSize(16384);

    // Verify config remains unchanged
    assertEquals("test.dat", config.getFilePath());
    assertEquals(2048, config.getDefaultBufferSize());
    assertEquals(4096, config.getMaxBufferSize());
  }

  @Test
  @DisplayName("Builder should handle null file path")
  void builderShouldHandleNullFilePath() {
    QueueConfig config = new QueueConfig.Builder()
        .filePath(null)
        .build();

    assertNull(config.getFilePath());
  }

  @Test
  @DisplayName("Builder should handle extreme values")
  void builderShouldHandleExtremeValues() {
    QueueConfig config = new QueueConfig.Builder()
        .maxFileSize(Long.MAX_VALUE)
        .defaultBufferSize(Integer.MAX_VALUE / 2)
        .maxBufferSize(Integer.MAX_VALUE)
        .batchThreshold(Integer.MAX_VALUE)
        .build();

    assertEquals(Long.MAX_VALUE, config.getMaxFileSize());
    assertEquals(Integer.MAX_VALUE / 2, config.getDefaultBufferSize());
    assertEquals(Integer.MAX_VALUE, config.getMaxBufferSize());
    assertEquals(Integer.MAX_VALUE, config.getBatchThreshold());
  }

  @Test
  @DisplayName("Builder should handle minimum valid values")
  void builderShouldHandleMinimumValues() {
    QueueConfig config = new QueueConfig.Builder()
        .maxFileSize(0)
        .defaultBufferSize(1)
        .maxBufferSize(1)
        .batchThreshold(1)
        .initialFileSize(PersistentMessageQueue.QUEUE_HEADER_SIZE)
        .build();

    assertEquals(0, config.getMaxFileSize());
    assertEquals(1, config.getDefaultBufferSize());
    assertEquals(1, config.getMaxBufferSize());
    assertEquals(1, config.getBatchThreshold());
    assertEquals(PersistentMessageQueue.QUEUE_HEADER_SIZE, config.getInitialFileSize());
  }

  @Test
  @DisplayName("Multiple builders should not interfere with each other")
  void multipleBuildersShouldNotInterfere() {
    QueueConfig.Builder builder1 = new QueueConfig.Builder()
        .filePath("queue1.dat")
        .defaultBufferSize(2048);

    QueueConfig.Builder builder2 = new QueueConfig.Builder()
        .filePath("queue2.dat")
        .defaultBufferSize(4096);

    QueueConfig config1 = builder1.build();
    QueueConfig config2 = builder2.build();

    assertEquals("queue1.dat", config1.getFilePath());
    assertEquals(2048, config1.getDefaultBufferSize());
    assertEquals("queue2.dat", config2.getFilePath());
    assertEquals(4096, config2.getDefaultBufferSize());
  }

  @Test
  @DisplayName("Builder should be reusable")
  void builderShouldBeReusable() {
    QueueConfig.Builder builder = new QueueConfig.Builder()
        .filePath("queue.dat")
        .defaultBufferSize(2048);

    QueueConfig config1 = builder.build();

    builder.filePath("queue2.dat")
        .defaultBufferSize(4096);

    QueueConfig config2 = builder.build();

    assertEquals("queue.dat", config1.getFilePath());
    assertEquals(2048, config1.getDefaultBufferSize());
    assertEquals("queue2.dat", config2.getFilePath());
    assertEquals(4096, config2.getDefaultBufferSize());
  }

  @Test
  @DisplayName("Multiple configurations should maintain independence")
  void multipleConfigsShouldMaintainIndependence() {
    QueueConfig.Builder builder = new QueueConfig.Builder();

    QueueConfig config1 = builder
        .filePath("queue1.dat")
        .defaultBufferSize(2048)
        .build();

    QueueConfig config2 = builder
        .filePath("queue2.dat")
        .defaultBufferSize(4096)
        .build();

    // Verify each config maintains its own values
    assertEquals("queue1.dat", config1.getFilePath());
    assertEquals(2048, config1.getDefaultBufferSize());
    assertEquals("queue2.dat", config2.getFilePath());
    assertEquals(4096, config2.getDefaultBufferSize());
  }
}