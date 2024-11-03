package io.github.elimelt.pmqueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.stream.Stream;

class MessageSerializerTest {

  private static Stream<Arguments> messageTestCases() {
    return Stream.of(
        Arguments.of("Empty message", new byte[0], 0),
        Arguments.of("Small message", "Hello".getBytes(), 1),
        Arguments.of("Large message", new byte[1024 * 1024], 2), // 1MB
        Arguments.of("Binary data", new byte[] { 0, 1, 2, 3, -1, -2, -3 }, 3));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("messageTestCases")
  @DisplayName("Serialize and deserialize should preserve message content")
  void serializeAndDeserializeShouldPreserveContent(String testName, byte[] data, int type) throws IOException {
    Message original = new Message(data, type);

    byte[] serialized = MessageSerializer.serialize(original);
    Message deserialized = MessageSerializer.deserialize(serialized);

    assertArrayEquals(original.getData(), deserialized.getData(),
        "Deserialized data should match original");
    assertEquals(original.getMessageType(), deserialized.getMessageType(),
        "Message type should be preserved");
  }

  @Test
  @DisplayName("Deserialize should reject invalid data")
  void deserializeShouldRejectInvalidData() {
    byte[] invalidData = "Not a valid serialized message".getBytes();
    assertThrows(IOException.class, () -> MessageSerializer.deserialize(invalidData));
  }

  @Test
  @DisplayName("Serialize should reject null message")
  void serializeShouldRejectNull() {
    assertThrows(IOException.class, () -> MessageSerializer.serialize(null));
  }

  @Test
  @DisplayName("Deserialize should handle empty array")
  void deserializeShouldHandleEmptyArray() {
    assertThrows(IOException.class, () -> MessageSerializer.deserialize(new byte[0]));
  }

  @Test
  @DisplayName("Serialization should handle messages with timestamp")
  void serializationShouldHandleTimestamp() throws IOException {
    Message original = new Message("test".getBytes(), 1);
    long originalTimestamp = original.getTimestamp();

    byte[] serialized = MessageSerializer.serialize(original);
    Message deserialized = MessageSerializer.deserialize(serialized);

    assertEquals(originalTimestamp, deserialized.getTimestamp(),
        "Timestamp should be preserved during serialization");
  }
}