package io.github.elimelt.pmqueue.message;

import org.junit.jupiter.api.Test;

import io.github.elimelt.pmqueue.message.Message;

import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

class MessageTest {

  @Test
  @DisplayName("Message constructor should correctly initialize fields")
  void constructorShouldInitializeFields() {
    byte[] data = "test data".getBytes();
    int messageType = 1;

    Message message = new Message(data, messageType);

    assertArrayEquals(data, message.getData());
    assertEquals(messageType, message.getMessageType());
    assertTrue(message.getTimestamp() > 0);
  }

  @Test
  @DisplayName("getData should return a copy of the data")
  void getDataShouldReturnCopy() {
    byte[] originalData = "test data".getBytes();
    Message message = new Message(originalData, 1);

    byte[] returnedData = message.getData();
    assertArrayEquals(originalData, returnedData);

    // modify returned data
    returnedData[0] = 42;

    // message remains unchanged
    assertNotEquals(returnedData[0], message.getData()[0]);
  }

  @Test
  @DisplayName("Constructor should create defensive copy of data")
  void constructorShouldCreateDefensiveCopy() {
    byte[] originalData = "test data".getBytes();
    Message message = new Message(originalData, 1);

    // modify original data
    originalData[0] = 42;

    // message remain unchanged
    assertNotEquals(originalData[0], message.getData()[0]);
  }

  @Test
  @DisplayName("Constructor should reject null data")
  void constructorShouldRejectNullData() {
    assertThrows(NullPointerException.class, () -> new Message(null, 1));
  }

  @Test
  @DisplayName("equals should return true for messages with the same data, timestamp, and type")
  void equalsShouldReturnTrueForEqualMessages() {
    byte[] data = "test data".getBytes();
    Message message1 = new Message(data, 1);
    Message message2 = new Message(data, 1);

    // constructed back-to-back with the same data and type; only differ if
    // they happen to straddle a system-clock millisecond tick
    assertEquals(message1, message2);
  }

  @Test
  @DisplayName("equals should return true for the same instance")
  void equalsShouldReturnTrueForSameInstance() {
    Message message = new Message("test data".getBytes(), 1);

    assertEquals(message, message);
  }

  @Test
  @DisplayName("equals should return false for messages with different data")
  void equalsShouldReturnFalseForDifferentData() {
    Message message1 = new Message("test data".getBytes(), 1);
    Message message2 = new Message("other data".getBytes(), 1);

    assertNotEquals(message1, message2);
  }

  @Test
  @DisplayName("equals should return false for messages with different message types")
  void equalsShouldReturnFalseForDifferentMessageType() {
    byte[] data = "test data".getBytes();
    Message message1 = new Message(data, 1);
    Message message2 = new Message(data, 2);

    assertNotEquals(message1, message2);
  }

  @Test
  @DisplayName("equals should return false for messages with different timestamps")
  void equalsShouldReturnFalseForDifferentTimestamps() throws InterruptedException {
    byte[] data = "test data".getBytes();
    Message message1 = new Message(data, 1);
    Thread.sleep(5);
    Message message2 = new Message(data, 1);

    assertNotEquals(message1.getTimestamp(), message2.getTimestamp());
    assertNotEquals(message1, message2);
  }

  @Test
  @DisplayName("equals should return false when compared to null or a different type")
  void equalsShouldReturnFalseForNullOrDifferentType() {
    Message message = new Message("test data".getBytes(), 1);

    assertNotEquals(null, message);
    assertNotEquals("not a message", message);
  }

  @Test
  @DisplayName("hashCode should be consistent with equals for equal messages")
  void hashCodeShouldBeConsistentForEqualMessages() {
    byte[] data = "test data".getBytes();
    Message message1 = new Message(data, 1);
    Message message2 = new Message(data, 1);

    assertEquals(message1, message2);
    assertEquals(message1.hashCode(), message2.hashCode());
  }

  @Test
  @DisplayName("hashCode should be stable across repeated calls")
  void hashCodeShouldBeStableAcrossCalls() {
    Message message = new Message("test data".getBytes(), 1);

    int firstCall = message.hashCode();
    int secondCall = message.hashCode();

    assertEquals(firstCall, secondCall);
  }
}