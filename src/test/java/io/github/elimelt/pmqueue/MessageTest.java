package io.github.elimelt.pmqueue;

import org.junit.jupiter.api.Test;
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
}